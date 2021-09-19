package main

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/GoGraph/block"
	"github.com/GoGraph/cache"
	"github.com/GoGraph/dp/internal/db"
	"github.com/GoGraph/ds"
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
	slog.Log("gql: ", s)
}

func main() {

	types.SetGraph("Movies")

	Startup()

	//clear monitor stats
	stat.ClearCh <- struct{}{}
	dpCh := make(chan db.DataT)

	runlimiter := grmgr.New("run", 1)
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
		fmt.Println("Waiting for processDPs to finish....")
		wg.Wait()
		fmt.Println("finished....")
	}

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
		nds [][][]byte
		ok  bool
		ty  string
		nc  *cache.NodeCache
		err error
	)
	gc := cache.GetCache()
	if len(ty_) == 0 {
		nc, err = gc.FetchForUpdate(pUID, "A#A#T")
		if err != nil {
			syslog(fmt.Sprintf("Error fetch type data for node: ", pUID.String()))
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

	// define a new transaction to handle all double propagation mutations for this node.
	ptx := tx.New(pUID.String())

	tyAttrs := types.TypeC.TyC[ty]

	for _, v := range tyAttrs {
		fmt.Println("In top loop : ", v.Ty)

		if v.Ty != "Performance" { // TODO: make generic i.e. check that Ty contains 1:1 uid-preds
			continue
		}

		psortk := "A#" + dpPart + "#:" + v.C
		fmt.Println("psortk: ,ty", psortk, ty)

		nc, err = gc.FetchForUpdate(pUID, psortk)
		if err != nil {
			syslog(fmt.Sprintf("No EOP entries for pUID: ", pUID.String()))
			panic(fmt.Errorf("FetchForUpdate error: %s", err.Error()))
		}
		//fmt.Println("about to .. UnmarshalNodeCache, result.tyS ", result.tyS)
		// fetch cUIDs for predicate with a 1:1 attribute by first generating an NV
		un := v.Name + ":"
		nv := &ds.NV{Name: un}
		fmt.Printf("NV : %#v\n", nv)
		nvc := make(ds.ClientNV, 1, 1)
		nvc[0] = nv

		err = nc.UnmarshalNodeCache(nvc, ty)
		if err != nil {
			panic(err)
		}
		// unmarshal will populate Value with chuild uids from parent uid-pred + from Overflow blocks

		if nds, ok = nvc[0].Value.([][][]byte); !ok {
			panic(fmt.Errorf("filterRootResult: data.Value is of wrong type")) // TODO: replace panic with error msg???
		}
		fmt.Println("len(nds) uids from paraent-uid-pred + overflow blocks : ", len(nds))
		fmt.Println("#children: ", len(nds))
		mutdml := mut.Insert

		for i, u := range nds {
			for k, uid := range u {
				fmt.Println(">>> i,k puid, cuid: ", i, k, pUID.String(), util.UID(uid).String(), mutdml)
				// fetch propagated scalar data from parant's uid-pred child node.
				ncc, err := gc.FetchNode(uid, "A#G#")
				if err != nil {
					panic(fmt.Errorf("FetchForUpdate error: %s", err.Error()))
				}
				for _, t := range types.TypeC.TyC[v.Ty] {
					// ignore scalar attributes and 1:M UID predicates
					if t.Card != "1:1" {
						continue
					}
					sk := "A#G#:" + t.C
					fmt.Println("child query sk: ", sk)
					for k, m := range ncc.GetMap() {
						if k == sk {
							// because of 1:1 there will only be one child uid for uid node.
							n, xf, _ := m.GetNd()
							xf_ := make([]int64, 1)
							xf[0] = block.ChildUID
							nl := make([]int, 1)
							nl[0] = 1
							v := make([][]byte, 1)
							v[0] = n[0]
							//fmt.Printf("PromoteUID: %s %s %T [%s] %v \n", psortk+"#"+sk[2:], k, m, n[1], xf[1])
							merge := mut.NewMutation(tbl.EOP, pUID, psortk+"#"+sk[2:], mutdml)
							merge.AddMember("Nd", v).AddMember("XF", xf_).AddMember("Id", nl)
							ptx.Add(merge)
						}
					}
					// for each attribute of the 1:1 predicate P.A, P.C, P.F
					for _, t_ := range types.TypeC.TyC[t.Ty] {

						compare := sk + "#:" + t_.C
						save := sk

						for k, m := range ncc.GetMap() {

							if k == compare {
								sk += "#:" + t_.C

								switch t_.DT {

								case "S":
									s, bl := m.GetULS()
									v := make([]string, 1, 1)
									// for 1:1 there will only be one entry in []string
									v[0] = s[0]
									nv := make([]bool, 1, 1)
									nv[0] = bl[0]
									merge := mut.NewMutation(tbl.EOP, pUID, psortk+"#"+sk[2:], mutdml)
									merge.AddMember("LS", v).AddMember("XBl", nv)
									ptx.Add(merge)

								case "I":
									s, bl := m.GetULI()
									v := make([]int64, 1, 1)
									v[0] = s[0]
									nv := make([]bool, 1, 1)
									nv[0] = bl[0]
									merge := mut.NewMutation(tbl.EOP, pUID, psortk+"#"+sk[2:], mutdml)
									merge.AddMember("LI", v).AddMember("XBl", nv)
									ptx.Add(merge)

								case "F":
									s, bl := m.GetULF()
									v := make([]float64, 1, 1)
									v[0] = s[0]
									nv := make([]bool, 1, 1)
									nv[0] = bl[0]
									merge := mut.NewMutation(tbl.EOP, pUID, psortk+"#"+sk[2:], mutdml)
									merge.AddMember("LF", v).AddMember("XBl", nv)
									ptx.Add(merge)

								case "B":
									s, bl := m.GetULB()
									v := make([][]byte, 1, 1)
									v[0] = s[0]
									nv := make([]bool, 1, 1)
									nv[0] = bl[0]
									merge := mut.NewMutation(tbl.EOP, pUID, psortk+"#"+sk[2:], mutdml)
									merge.AddMember("LB", v).AddMember("XBl", nv)
									ptx.Add(merge)

								case "Bl":
									s, bl := m.GetULBl()
									v := make([]bool, 1, 1)
									v[0] = s[0]
									nv := make([]bool, 1, 1)
									nv[0] = bl[0]
									merge := mut.NewMutation(tbl.EOP, pUID, psortk+"#"+sk[2:], mutdml)
									merge.AddMember("LBl", v).AddMember("XBl", nv)
									ptx.Add(merge)
								}
							}
							sk = save
						}
					}
				}
				// if err := ptx.MakeBatch(); err != nil {
				// 	elog.Add(logid, err)
				// }
				mutdml = mut.Update
				ncc.Unlock()
			}

		}
	}
	//
	// post: update IX to Y
	//
	//	db.UpdateIX(pUID)
	merge := mut.NewMutation(tbl.Block, pUID, "", mut.Update)
	merge.AddMember("IX", "Y")
	ptx.Add(merge)
	//ptx.MakeBatch() // will be preformed in Execute anyway

	err = ptx.Execute()

	if err != nil {
		elog.Add(logid, err)
	}

	gc.ClearNodeCache(pUID)

}
