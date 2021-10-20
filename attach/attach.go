package main

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/GoGraph/attach/anmgr"
	"github.com/GoGraph/attach/db"
	"github.com/GoGraph/attach/ds"
	"github.com/GoGraph/attach/execute"
	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/errlog"
	"github.com/GoGraph/grmgr"
	"github.com/GoGraph/run"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/types"
	//"github.com/GoGraph/tbl"
	//"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/util"
)

const (
	logid = param.Logid
)

var (
	ctx    context.Context
	wpEnd  sync.WaitGroup
	cancel context.CancelFunc
)

func syslog(s string) {
	slog.Log(logid, s)
}

//var attachers = flag.Int("a", 1, "Attachers: ")

var debug = flag.Int("debug", 0, "Enable full logging [ 1: enable] 0: disable")
var attachers = flag.Int("c", 6, "# parallel goroutines")
var graph = flag.String("g", "", "Graph: ")
var showsql = flag.Int("sql", 0, "Show generated SQL [1: enable 0: disable]")
var reduceLog = flag.Int("rlog", 1, "Reduced Logging [1: enable 0: disable]")

var runId int64

func GetRunId() int64 {
	return runId
}

func main() {

	flag.Parse()
	param.DebugOn = true
	syslog(fmt.Sprintf("Argument: concurrency: %d", *attachers))
	syslog(fmt.Sprintf("Argument: showsql: %v", *showsql))
	syslog(fmt.Sprintf("Argument: debug: %v", *debug))
	syslog(fmt.Sprintf("Argument: graph: %s", *graph))
	syslog(fmt.Sprintf("Argument: reduced logging: %v", *reduceLog))

	fmt.Printf("Argument: concurrent: %d\n", *attachers)
	fmt.Printf("Argument: showsql: %v\n", *showsql)
	fmt.Printf("Argument: debug: %v\n", *debug)
	fmt.Printf("Argument: graph: %s\n", *graph)
	fmt.Printf("Argument: reduced logging: %v\n", *reduceLog)
	var (
		edgeCh         chan *ds.Edge
		wpEnd, wpStart sync.WaitGroup
		runNow         bool // whether to run attachNode on current edge
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
	runid, err = run.New(logid, "attacher")
	if err != nil {
		fmt.Println(fmt.Sprintf("Error in  MakeRunId() : %s", err))
		return
	}
	defer run.Finish(err)

	ctx, cancel := context.WithCancel(context.Background())
	edgeCh = make(chan *ds.Edge, 2)

	wpEnd.Add(4)
	wpStart.Add(4)
	// supporting routine
	go sourceEdge(ctx, &wpStart, &wpEnd, edgeCh)
	// services
	//go stop.PowerOn(ctx, &wpStart, &wpEnd)    // detect a kill action (?) to terminate program alt just kill-9 it.
	go grmgr.PowerOn(ctx, &wpStart, &wpEnd, runid) // concurrent goroutine manager service
	go errlog.PowerOn(ctx, &wpStart, &wpEnd)       // error logging service
	go anmgr.PowerOn(ctx, &wpStart, &wpEnd)        // attach node service
	// Dynamodb only: go monitor.PowerOn(ctx, &wpStart, &wpEnd)      // repository of system statistics service
	wpStart.Wait()

	syslog("All services started. Proceed with attach processing")
	t0 := time.Now()

	err = types.SetGraph(*graph)
	if err != nil {
		syslog(fmt.Sprintf("Error in SetGraph: %s ", err.Error()))
		fmt.Printf("Error in SetGraph: %s\n", err)
		return
	}

	limiterAttach := grmgr.New("nodeAttach", *attachers)

	respch := make(chan bool)

	var (
		edges = 0
		wg    sync.WaitGroup
	)

	for e := range edgeCh {

		e.RespCh = respch
		anmgr.AttachNowCh <- e
		runNow = <-e.RespCh

		if runNow {

			op := AttachOp{Puid: e.Puid, Cuid: e.Cuid, Sortk: e.Sortk}

			limiterAttach.Ask()
			<-limiterAttach.RespCh()
			edges++
			wg.Add(1)

			go execute.AttachNode(util.UID(e.Cuid), util.UID(e.Puid), e.Sortk, e, &wg, limiterAttach, &op)
		}

	}
	wg.Wait()
	t1 := time.Now()
	limiterAttach.Unregister()
	// send cancel to all registered goroutines
	cancel()
	wpEnd.Wait()

	syslog(fmt.Sprintf("Attach operation finished. Edges: %d  Duration: %s ", edges, t1.Sub(t0)))
}

func sourceEdge(ctx context.Context, wp *sync.WaitGroup, wgEnd *sync.WaitGroup, edgeCh chan<- *ds.Edge) {

	defer wgEnd.Done()
	wp.Done()

	const logid = "SourceEdge:"

	slog.Log(logid, "Powering on...")

	for {

		var err error

		ns, eof := db.ScanForNodes()

		for _, n := range ns {

			edge, err := db.FetchEdge(n)
			if err != nil {
				if err.Error() == "No matching EdgeChild" {
					continue
				}
				err = err
				break
			}
			edgeCh <- edge
		}
		if err != nil {
			errlog.Add(logid, fmt.Errorf("Error in FetchEdge: %w", err))
			break
		}
		if eof {
			slog.Log(logid, "eof ... break")
			break
		}
		// poll for cancel msg
		select {
		case <-ctx.Done():
			slog.Log(logid, "Premature shutdown")
			break
		default:
		}
	}
	close(edgeCh)
	slog.Log(logid, "Powering off....")
}
