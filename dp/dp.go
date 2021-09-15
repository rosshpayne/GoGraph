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
	for _, attr := range []string{"P", "Fm"} { // types containing nested 1:1 type

		// start scan service
		go db.ScanForDPitems(attr, dpCh)

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
			fmt.Printf("\t\t\t %g\n", c)
			runlimiter.Ask()
			<-runlimiter.RespCh()
			wg.Add(1)

			go processDP(runlimiter, &wg, v.Uid)

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
	wpStart.Add(2)
	// check verify and saveNode have finished. Each goroutine is responsible for closing and waiting for all routines they spawn.
	ctxEnd.Add(2)
	// l := lexer.New(input)
	// p := New(l)
	//
	// context - used to shutdown goroutines that are not part fo the pipeline
	//
	ctx, cancel = context.WithCancel(context.Background())

	go grmgr.PowerOn(ctx, &wpStart, &ctxEnd)
	go stat.PowerOn(ctx, &wpStart, &ctxEnd)

	wpStart.Wait()
	syslog(fmt.Sprintf("services started "))

	return ctx
}

func Shutdown() {

	syslog("Shutdown commenced...")
	cancel()

	ctxEnd.Wait()
	syslog("Shutdown complete")
}

func processDP(limit *grmgr.Limiter, wg *sync.WaitGroup, pUID util.UID) {

	defer limit.EndR()
	defer wg.Done()

	gc := cache.GetCache()
	// fetch all parent node UID-PREDs - however there maybe only one we are interested in
	nc, err := gc.FetchForUpdate(pUID, "A#G#")
	if err != nil {
		syslog(fmt.Sprintf("No EOP entries for pUID: ", pUID.String()))
		panic(fmt.Errorf("FetchForUpdate error: %s", err.Error()))
	}
	// p's type
	fmt.Println("ABout to GetType for ", pUID.String())
	ty, _ := nc.GetType()
	fmt.Println("Type is ", pUID.String(), ty)
	//
	// for each performance type in p
	//
	tyAttrs := types.TypeC.TyC[ty]
	//
	// for all performance types in p
	//
	var (
		nds [][][]byte
		ok  bool
	)
	ptx := tx.New(pUID.String())
	// for each attribute in p's type - find the 1:1 attribute
	//fmt.Printf("types.TypeC.Ty %#v", types.TypeC.TyC)
	// for k, v := range types.TypeC.TyC {
	// 	fmt.Println(k)
	// 	fmt.Printf("%#v\n", v)
	// 	fmt.Println(" -- ")
	// }
	for _, v := range tyAttrs {
		fmt.Println("In top loop : ", v.Ty)
		if v.Ty != "Performance" {
			continue
		}
		// fetch cUIDs for predicate with a 1:1 attribute by first generating an NV
		un := v.Name + ":"
		nv := &ds.NV{Name: un}
		nvc := make(ds.ClientNV, 1, 1)
		nvc[0] = nv
		psortk := "A#" + dpPart + "#:" + v.C
		fmt.Println("psortk: ,ty", psortk, ty)
		//
		// assign cached data to NV
		//
		//fmt.Println("about to .. UnmarshalNodeCache, result.tyS ", result.tyS)
		err = nc.UnmarshalNodeCache(nvc, ty)
		if err != nil {
			panic(err)
		}
		//
		//
		data := nvc[0].Value
		if nds, ok = data.([][][]byte); !ok {
			panic(fmt.Errorf("filterRootResult: data.Value is of wrong type")) // TODO: replace panic with error msg???
		}
		fmt.Println("len(nds) : ", len(nds))
		// fetch child UIDs for 1:1 predicate film->performance(uids) fetch all promoted data which represents the grandchild node scalar data
		// eventhough some of these may not be relevant i.e. not all 1:1 (TODO: refine this query to only 1:1)
		for _, u := range nds {
			// split nd ranges (across Overflow blocks)

			for i, uid := range u {
				mutType := mut.Update
				if i == 0 {
					mutType = mut.Insert
				}

				fmt.Println(">>> uid: ", util.UID(uid).String())

				ncc, err := gc.FetchNode(uid, "A#G#")
				if err != nil {
					panic(fmt.Errorf("FetchForUpdate error: %s", err.Error()))
				}
				// for k, _ := range ncc.GetMap() {
				// 	fmt.Println("child sortk: ", k)
				// }
				ty := v.Ty // "Performance" // 1:1 attribute
				// now copy the promoted data in the 1:1 predicate (which presents the grantchild node data)
				// (in performance->actor (Name), performane.Character (Name), performance.Filme (Name, Revenue, lastreleasedate))
				for _, t := range types.TypeC.TyC[ty] {
					// ignore scalar attributes and 1:M UID predicates
					if t.Card != "1:1" {
						continue
					}
					sk := "A#G#:" + t.C
					fmt.Println("child query sk: ", sk)
					for k, m := range ncc.GetMap() {
						if k == sk {
							n, xf, _ := m.GetNd()
							xf_ := make([]int64, 1)
							xf[0] = block.ChildUID
							nl := make([]int, 1)
							nl[0] = 1
							v := make([][]byte, 1)
							v[0] = n[0]
							//fmt.Printf("PromoteUID: %s %s %T [%s] %v \n", psortk+"#"+sk[2:], k, m, n[1], xf[1])
							merge := mut.NewMutation(tbl.EOP, pUID, psortk+"#"+sk[2:], mutType)
							merge.AddMember("Nd", v).AddMember("XF", xf_).AddMember("Id", nl)
							ptx.Add(merge)
						}
					}
					// for each attribute of the 1:1 predicate
					for _, t_ := range types.TypeC.TyC[t.Ty] {
						//
						compare := sk + "#:" + t_.C
						save := sk

						for k, m := range ncc.GetMap() {
							fmt.Printf("k,m: %s %#v\n", k, m)
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
									merge := mut.NewMutation(tbl.EOP, pUID, psortk+"#"+sk[2:], mutType)
									merge.AddMember("LS", v).AddMember("XBl", nv)
									ptx.Add(merge)

								case "I":
									s, bl := m.GetULI()
									v := make([]int64, 1, 1)
									v[0] = s[0]
									nv := make([]bool, 1, 1)
									nv[0] = bl[0]
									merge := mut.NewMutation(tbl.EOP, pUID, psortk+"#"+sk[2:], mutType)
									merge.AddMember("LI", v).AddMember("XBl", nv)
									ptx.Add(merge)

								case "F":
									s, bl := m.GetULF()
									v := make([]float64, 1, 1)
									v[0] = s[0]
									nv := make([]bool, 1, 1)
									nv[0] = bl[0]
									merge := mut.NewMutation(tbl.EOP, pUID, psortk+"#"+sk[2:], mutType)
									merge.AddMember("LF", v).AddMember("XBl", nv)
									ptx.Add(merge)

								case "B":
									s, bl := m.GetULB()
									v := make([][]byte, 1, 1)
									v[0] = s[0]
									nv := make([]bool, 1, 1)
									nv[0] = bl[0]
									merge := mut.NewMutation(tbl.EOP, pUID, psortk+"#"+sk[2:], mutType)
									merge.AddMember("LB", v).AddMember("XBl", nv)
									ptx.Add(merge)

								case "Bl":
									s, bl := m.GetULBl()
									v := make([]bool, 1, 1)
									v[0] = s[0]
									nv := make([]bool, 1, 1)
									nv[0] = bl[0]
									merge := mut.NewMutation(tbl.EOP, pUID, psortk+"#"+sk[2:], mutType)
									merge.AddMember("LBl", v).AddMember("XBl", nv)
									ptx.Add(merge)
								}
							}
							sk = save
						}
					}
				}
				ncc.Unlock()

				err = ptx.Execute()

				if err != nil {
					syslog(err.Error())
					panic(fmt.Errorf("Error in processDP: %s", err))
				}

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

	err = ptx.Execute()

	if err != nil {
		syslog(err.Error())
		panic(fmt.Errorf("Error in processDP: %s", err))
	}

	nc.ClearNodeCache()

}
