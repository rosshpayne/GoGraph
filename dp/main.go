package main

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/GoGraph/attach/anmgr"
	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/errlog"
	"github.com/GoGraph/grmgr"
	"github.com/GoGraph/run"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/types"
)

const (
	logid = param.Logid
)

var debug = flag.Int("debug", 0, "Enable full logging [ 1: enable] 0: disable")
var parallel = flag.Int("c", 6, "# parallel goroutines")
var graph = flag.String("g", "", "Graph: ")
var showsql = flag.Int("sql", 0, "Show generated SQL [1: enable 0: disable]")
var reduceLog = flag.Int("rlog", 1, "Reduced Logging [1: enable 0: disable]")

var runId int64

func syslog(s string) {
	slog.Log(logid, s)
}

func main() {
	// determine types which reference types that have a cardinality of 1:1
	flag.Parse()
	param.DebugOn = true
	syslog(fmt.Sprintf("Argument: concurrency: %d", *parallel))
	syslog(fmt.Sprintf("Argument: showsql: %v", *showsql))
	syslog(fmt.Sprintf("Argument: debug: %v", *debug))
	syslog(fmt.Sprintf("Argument: graph: %s", *graph))
	syslog(fmt.Sprintf("Argument: reduced logging: %v", *reduceLog))

	fmt.Printf("Argument: concurrent: %d\n", *parallel)
	fmt.Printf("Argument: showsql: %v\n", *showsql)
	fmt.Printf("Argument: debug: %v\n", *debug)
	fmt.Printf("Argument: graph: %s\n", *graph)
	fmt.Printf("Argument: reduced logging: %v\n", *reduceLog)
	var (
		wpEnd, wpStart sync.WaitGroup
		err            error
		runid          int64
	)

	// allocate a run id
	// allocate a run id

	param.ReducedLog = false
	if *reduceLog == 1 {
		param.ReducedLog = true
	}
	if *showsql == 1 {
		param.ShowSQL = true
	}
	if *debug == 1 {
		param.DebugOn = true
	}
	//
	// set graph to use
	//
	if len(*graph) == 0 {
		fmt.Printf("Must supply a graph name\n")
		flag.PrintDefaults()
		return
	}
	runid, err = run.New(logid, "dp")
	if err != nil {
		fmt.Println(fmt.Sprintf("Error in  MakeRunId() : %s", err))
		return
	}
	syslog(fmt.Sprintf("runid: %d", runid))
	if runid < 1 {
		fmt.Printf("Abort: runid is invalid %d\n", runid)
		return
	}
	defer run.Finish(err)

	ctx, cancel := context.WithCancel(context.Background())

	wpEnd.Add(3)
	wpStart.Add(3)

	// services
	//go stop.PowerOn(ctx, &wpStart, &wpEnd)    // detect a kill action (?) to terminate program alt just kill-9 it.
	go grmgr.PowerOn(ctx, &wpStart, &wpEnd, runid) // concurrent goroutine manager service
	go errlog.PowerOn(ctx, &wpStart, &wpEnd)       // error logging service
	go anmgr.PowerOn(ctx, &wpStart, &wpEnd)        // attach node service
	// Dynamodb only: go monitor.PowerOn(ctx, &wpStart, &wpEnd)      // repository of system statistics service
	wpStart.Wait()

	syslog("All services started. Proceed with attach processing")

	err = types.SetGraph(*graph)
	if err != nil {
		syslog(fmt.Sprintf("Error in SetGraph: %s ", err.Error()))
		fmt.Printf("Error in SetGraph: %s\n", err)
		return
	}

	has11 := make(map[string]struct{})
	dpTy := make(map[string]struct{})

	for k, v := range types.TypeC.TyC {
		for _, vv := range v {
			if vv.Ty == "" {
				continue
			}
			if _, ok := has11[k]; ok {
				break
			}
			if vv.Card == "1:1" {
				has11[k] = struct{}{}
			}
		}
	}
	for k, v := range types.TypeC.TyC {
		for _, vv := range v {
			if _, ok := has11[vv.Ty]; ok {
				if sn, ok := types.GetTyShortNm(k); ok {
					dpTy[sn] = struct{}{}
				}
			}
		}
	}

	var wgc sync.WaitGroup
	limiterDP := grmgr.New("dp", *parallel)

	for k, _ := range dpTy {
		syslog(fmt.Sprintf(" Type containing 1:1 type: %s", k))
	}
	syslog("Start double propagation processing...")

	t0 := time.Now()
	for ty, _ := range dpTy {

		ty := ty
		for n := range FetchNodeCh(ty) {

			n := n
			limiterDP.Ask()
			<-limiterDP.RespCh()
			wgc.Add(1)

			go Propagate(limiterDP, &wgc, n, ty, has11)

		}
		wgc.Wait()
	}
	wgc.Wait()
	t1 := time.Now()
	limiterDP.Unregister()
	cancel()
	wpEnd.Wait()

	syslog(fmt.Sprintf("double propagate processing finished. Duration: %s", t1.Sub(t0)))

}
