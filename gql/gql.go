package gql

import (
	"context"
	"fmt"
	"sync"
	"time"

	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/gql/ast"
	stat "github.com/GoGraph/gql/monitor"
	"github.com/GoGraph/gql/parser"
	slog "github.com/GoGraph/syslog"
)

var (
	ctx    context.Context
	cancel context.CancelFunc
	ctxEnd sync.WaitGroup
	//
	replyCh        chan interface{}
	statTouchNodes stat.Request
	statTouchLvl   stat.Request
	statDbFetches  stat.Request
	//
	expectedJSON       string
	expectedTouchLvl   []int
	expectedTouchNodes int
	//
	t0, t1, t2 time.Time
)

const logid = param.Logid

func syslog(s string) {
	slog.Log(logid, s)
}

func init() {

	replyCh = make(chan interface{})
	statTouchNodes = stat.Request{Id: stat.TouchNodeFiltered, ReplyCh: replyCh}
	statTouchLvl = stat.Request{Id: stat.TouchLvlFiltered, ReplyCh: replyCh}
	statDbFetches = stat.Request{Id: stat.NodeFetch, ReplyCh: replyCh}

	Startup()

	fmt.Println("====================== STARTUP =====================")
}

func Execute(graph string, query string) *ast.RootStmt {

	//clear monitor stats
	stat.ClearCh <- struct{}{}

	t0 = time.Now()
	p := parser.New(graph, query)
	stmt, errs := p.ParseInput()
	if len(errs) > 0 {
		for _, v := range errs {
			fmt.Println("error: ", v)
		}
		panic(errs[0])
	}
	//
	t1 = time.Now()
	stmt.Execute()
	t2 = time.Now()

	fmt.Printf("Duration:  Parse  %s  Execute: %s    \n", t1.Sub(t0), t2.Sub(t1))
	syslog(fmt.Sprintf("Duration: Parse  %s  Execute: %s ", t1.Sub(t0), t2.Sub(t1)))
	time.Sleep(2 * time.Second) // give time for stat to empty its channel queues

	stat.PrintCh <- struct{}{}
	//Shutdown()

	return stmt

}

func Startup() {

	var (
		wpStart sync.WaitGroup
	)
	syslog("Startup...")
	wpStart.Add(1)
	// check verify and saveNode have finished. Each goroutine is responsible for closing and waiting for all routines they spawn.
	ctxEnd.Add(1)
	// l := lexer.New(input)
	// p := New(l)
	//
	// context - used to shutdown goroutines that are not part fo the pipeline
	//
	ctx, cancel = context.WithCancel(context.Background())

	go stat.PowerOn(ctx, &wpStart, &ctxEnd)

	wpStart.Wait()
	syslog(fmt.Sprintf("services started "))
}

func Shutdown() {

	syslog("Shutdown commenced...")
	cancel()

	ctxEnd.Wait()
	syslog("Shutdown Completed")
}
