package main

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	blk "github.com/GoGraph/block"
	"github.com/GoGraph/cache"
	"github.com/GoGraph/dp/internal/db"
	stat "github.com/GoGraph/gql/monitor"
	elog "github.com/GoGraph/rdf/errlog"
	"github.com/GoGraph/rdf/grmgr"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/types"
	"github.com/GoGraph/util"
)

const (
	dpPart = "G"
	logid  = "processDP:"
)

var (
	ctx    context.Context
	ctxEnd sync.WaitGroup
	cancel context.CancelFunc
)

func syslog(s string) {
	slog.Log("dp: ", s)
}

func main() {

	types.SetGraph("Movies")

	Startup()

	//clear monitor stats
	stat.ClearCh <- struct{}{}
	dpCh := make(chan db.DataT)

	runlimiter := grmgr.New("dprun", 1)
	//
	var c float64
	var wg sync.WaitGroup
	// determine existence of in-direct (UID-only) nodes
	// determine what nodes have attributes of this type (indirect nodes)
	for i, attr := range []string{"P", "Fm"} { // types containing nested 1:1 type

		i := i
		// start scan service
		go db.ScanForDPitems(attr, dpCh, i)

		for v := range dpCh {

			if v.Eod {
				fmt.Println("============== EOD ============")
				break
			}
			fmt.Println("Process Uid: ", v.Uid.String())
			c++
			if math.Mod(c, 100) == 0 {
				fmt.Printf("\t\t\t %g\n", c)
			}
			runlimiter.Ask()
			<-runlimiter.RespCh()
			wg.Add(1)

			go processDP(runlimiter, &wg, v.Uid, attr)
		}

		wg.Wait()
	}
	runlimiter.Finish()

	Shutdown()

}

func Startup() context.Context {

	var (
		wpStart sync.WaitGroup
	)
	syslog("Startup...")
	wpStart.Add(3)
	// check verify and saveNode have finished. Each goroutine is responsible for closing and waiting for all routines they spawn.
	ctxEnd.Add(3)
	// l := lexer.New(input)
	// p := New(l)
	//
	// context - used to shutdown goroutines that are not part fo the pipeline
	//
	ctx, cancel = context.WithCancel(context.Background())

	go grmgr.PowerOn(ctx, &wpStart, &ctxEnd)
	go stat.PowerOn(ctx, &wpStart, &ctxEnd)
	go elog.PowerOn(ctx, &wpStart, &ctxEnd) // error logging service

	wpStart.Wait()
	syslog(fmt.Sprintf("services started "))

	return ctx
}

func Shutdown() {

	printErrors()
	syslog("Shutdown commenced...")
	cancel()

	ctxEnd.Wait()
	syslog("Shutdown complete")

}

func printErrors() {

	elog.ReqErrCh <- struct{}{}
	errs := <-elog.GetErrCh
	syslog(fmt.Sprintf(" ==================== ERRORS : %d	==============", len(errs)))
	fmt.Printf(" ==================== ERRORS : %d	==============\n", len(errs))
	if len(errs) > 0 {
		for _, e := range errs {
			syslog(fmt.Sprintf(" %s:  %s", e.Id, e.Err))
			fmt.Println(e.Id, e.Err)
		}
	}
}

func processDP(limit *grmgr.Limiter, wg *sync.WaitGroup, pUID util.UID, ty_ ...string) {

	defer limit.EndR()
	defer wg.Done()

	var (
		ty  string
		nc  *cache.NodeCache
		err error
		wgc sync.WaitGroup
	)
	gc := cache.GetCache()
	if len(ty_) == 0 {
		nc, err = gc.FetchNode(pUID, "A#A#T")
		if err != nil {
			panic(fmt.Errorf("FetchForUpdate error: %s", err.Error()))
		}
		// p's type
		ty, _ = nc.GetType()
	} else {
		var b bool
		ty, b = types.GetTyLongNm(ty_[0])
		if b == false {
			panic(fmt.Errorf("cache.GetType() errored. Could not find long type name for short name %s", ty_[0]))
		}
	}

	tyAttrs := types.TypeC.TyC[ty]

	for _, v := range tyAttrs {
		syslog(fmt.Sprintln("In top loop : ", v.Ty))

		v := v
		if v.Ty != "Performance" { // TODO: make generic i.e. check that Ty contains 1:1 uid-preds
			continue
		}

		psortk := "A#" + dpPart + "#:" + v.C
		// lock parent node and load performance UID-PRED into cache
		nc, err = gc.FetchForUpdate(pUID, psortk)
		if err != nil {
			panic(fmt.Errorf("FetchForUpdate error: %s", err.Error()))
		}
		// unmarshal uid-pred returning  slice of channels to read overflow batches containing an array of cUIDs.
		bChs := nc.UnmarshalEdge(psortk)

		// create a goroutine to read from each channel created in UnmarshalEdge()
		for i, k := range bChs {

			var (
				nd     [][]byte
				xf     []int64
				mutdml mut.StdDML
			)
			ii, kk := i, k
			wgc.Add(1)

			go func(oid int, rch <-chan cache.BatchPy, v blk.TyAttrD) {

				defer wgc.Done()
				// make a grmgr label

				var blimiter *grmgr.Limiter
				if oid == 0 {
					blimiter = nil
				} else {
					blimiter = grmgr.New(pUID.String()[:8], 2)
				}
				var wgd sync.WaitGroup
				// a channel for embedded + each oblock. Each oblock batch is sent on this channel
				for py := range rch {

					syslog(fmt.Sprintf("** Reading from batch channel B = %d", py.Batch))
					if blimiter != nil {
						blimiter.Ask()
						<-blimiter.RespCh()
						syslog(fmt.Sprintf("** Process B = %d", py.Batch))
						wgd.Add(1)
					}
					// a goroutine for each batch
					go func(py cache.BatchPy, v blk.TyAttrD) {

						if blimiter != nil {
							defer wgd.Done()
							defer blimiter.EndR()
						}
						// transaction for each channel. TODO: log failure
						ptx := tx.New(pUID.String() + "-" + strconv.Itoa(oid))

						pUID := py.Puid // either parent node or overflow UID
						switch py.Bid {
						case 0: //embedded
							nd, xf, _ = py.DI.GetNd()
						default: // overflow batch
							nd, xf = py.DI.GetOfNd()
						}
						mutdml = mut.Insert
						// process each cuid within embedded or each overflow batch array
						// only handle 1:1 attribute types which means only one entity defined in propagated data
						for _, cuid := range nd {

							// fetch 1:1 node propagated data and assign to pnode
							// load cache with node's uid-pred and propagated data
							ncc, err := gc.FetchNode(cuid, "A#G#")
							if err != nil {
								panic(fmt.Errorf("FetchNode error: %s", err.Error()))
							}
							// fetch propagated scalar data from parant's uid-pred child node.

							for _, t := range types.TypeC.TyC[v.Ty] {

								// ignore scalar attributes and 1:M UID predicates
								if t.Card != "1:1" {
									continue
								}
								sk := "A#G#:" + t.C // Actor, Character, Film UUIDs [uid-pred]
								var (
									psk string
								)

								switch py.Batch {
								case 0: // batchId 0 is embedded cuids
									psk = psortk + "#" + sk[2:]
								default: // overflow
									psk = psortk + "%" + strconv.Itoa(py.Batch) + "#" + sk[2:]
								}
								for k, m := range ncc.GetMap() {
									//search for uid-pred entry in cache
									if k == sk {
										// because of 1:1 there will only be one child uid for uid node.
										n, xf, _ := m.GetNd()
										xf_ := make([]int64, 1)
										xf_[0] = xf[0] //block.ChildUID
										v := make([][]byte, 1)
										v[0] = n[0]
										//fmt.Printf("PromoteUID: %s %s %T [%s] %v \n", psortk+"#"+sk[2:], k, m, n[1], xf[1])
										merge := mut.NewMutation(tbl.EOP, pUID, psk, mutdml)
										merge.AddMember("Nd", v).AddMember("XF", xf_) //.AddMember("Id", nl)
										ptx.Add(merge)
									}
								}
								// for each attribute of the 1:1 predicate P.A, P.C, P.F
								for _, t_ := range types.TypeC.TyC[t.Ty] {

									// find propagated scalar in cache
									compare := sk + "#:" + t_.C

									for k, m := range ncc.GetMap() {

										if k == compare {

											sk := sk + "#:" + t_.C
											switch py.Batch {
											case 0: // batchId 0 is embedded cuids
												psk = psortk + "#" + sk[2:]
											default: // overflow
												psk = psortk + "%" + strconv.Itoa(py.Batch) + "#" + sk[2:]
											}

											switch t_.DT {

											case "S":
												s, bl := m.GetULS()
												v := make([]string, 1, 1)
												// for 1:1 there will only be one entry in []string
												v[0] = s[0]
												nv := make([]bool, 1, 1)
												nv[0] = bl[0]
												merge := mut.NewMutation(tbl.EOP, pUID, psk, mutdml)
												merge.AddMember("LS", v).AddMember("XBl", nv)
												ptx.Add(merge)

											case "I":
												s, bl := m.GetULI()
												v := make([]int64, 1, 1)
												v[0] = s[0]
												nv := make([]bool, 1, 1)
												nv[0] = bl[0]
												merge := mut.NewMutation(tbl.EOP, pUID, psk, mutdml)
												merge.AddMember("LI", v).AddMember("XBl", nv)
												ptx.Add(merge)

											case "F":
												s, bl := m.GetULF()
												v := make([]float64, 1, 1)
												v[0] = s[0]
												nv := make([]bool, 1, 1)
												nv[0] = bl[0]
												merge := mut.NewMutation(tbl.EOP, pUID, psk, mutdml)
												merge.AddMember("LF", v).AddMember("XBl", nv)
												ptx.Add(merge)

											case "B":
												s, bl := m.GetULB()
												v := make([][]byte, 1, 1)
												v[0] = s[0]
												nv := make([]bool, 1, 1)
												nv[0] = bl[0]
												merge := mut.NewMutation(tbl.EOP, pUID, psk, mutdml)
												merge.AddMember("LB", v).AddMember("XBl", nv)
												ptx.Add(merge)

											case "Bl":
												s, bl := m.GetULBl()
												v := make([]bool, 1, 1)
												v[0] = s[0]
												nv := make([]bool, 1, 1)
												nv[0] = bl[0]
												merge := mut.NewMutation(tbl.EOP, pUID, psk, mutdml)
												merge.AddMember("LBl", v).AddMember("XBl", nv)
												ptx.Add(merge)
											}
										}
									}
								}
							}
							// if err := ptx.MakeBatch(); err != nil {
							// 	elog.Add(logid, err)
							// }
							mutdml = mut.Update
							ncc.Unlock()
						}

						// commit each batch separately - TODO: log sucess against batch id for pUID
						err = ptx.Execute()
						if err != nil {
							if strings.HasPrefix(err.Error(), "No mutations in transaction") {
								syslog(err.Error())
							} else {
								elog.Add(logid, err)
							}
						}
					}(py, v)

				}
				syslog("Waiting on wgd...")
				wgd.Wait()
				syslog("Finished waiting on wgd...")
				if blimiter != nil {
					blimiter.Finish()
				}

			}(ii, kk, v)

			wgc.Wait()
		}

	}
	//
	// post: update IX to Y
	//
	//	db.UpdateIX(pUID)
	ptx := tx.New("IXFlag")
	merge := mut.NewMutation(tbl.Block, pUID, "", mut.Update)
	merge.AddMember("IX", "Y")
	ptx.Add(merge)
	//ptx.MakeBatch() // will be preformed in Execute anyway

	ptx.Execute()

	if err != nil {
		if strings.HasPrefix(err.Error(), "No mutations in transaction") {
			fmt.Println(err.Error())
		} else {
			elog.Add(logid, err)
		}
	}

	gc.ClearNodeCache(pUID)

}
