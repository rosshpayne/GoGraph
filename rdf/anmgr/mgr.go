package anmgr

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/GoGraph/rdf/uuid"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/util"
)

const (
	LogLabel   = "anmgr: "
	eBatchSize = 1000
	eGCfreq    = 4
)

type EdgeSn struct {
	CSn   string
	PSn   string
	Sortk string
}

type EdgeSnStore struct {
	j, k  int
	store [][]EdgeSn
}

type Edge struct {
	Cuid     util.UID //[]byte
	Puid     util.UID //[]byte
	Sortk    string
	attached bool
}

type edgeKey struct {
	CuidS string
	PuidS string
	Sortk string
}

var (
	edges  []*Edge
	edges_ EdgeSnStore
)

type attachRunningMap map[edgeKey]bool // set of running attachNodes
var attachRunning attachRunningMap

//type attachDoneMap map[EdgeSn]bool // set of completed attachNodes
var attachDone int //attachDoneMap

var (
	EdgeSnCh     chan EdgeSn
	JoinNodes    chan struct{}
	AttachNodeCh chan *Edge
	attachDoneCh chan *Edge //EdgeSn
)

func init() {
	EdgeSnCh = make(chan EdgeSn)
	JoinNodes = make(chan struct{})
	AttachNodeCh = make(chan *Edge)
	attachDoneCh = make(chan *Edge, 1) //EdgeSn, 1)
}

func AttachDone(e *Edge) { //EdgeSn) {
	attachDoneCh <- e
}

func printMemUsage() string {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	var s strings.Builder
	s.WriteString(fmt.Sprintf("Alloc = %v MiB", bToMb(m.Alloc)))
	s.WriteString(fmt.Sprintf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc)))
	s.WriteString(fmt.Sprintf("\tSys = %v MiB", bToMb(m.Sys)))
	s.WriteString(fmt.Sprintf("\tNumGC = %v\n", m.NumGC))
	s.WriteString(fmt.Sprintf("\tFrees = %d\n", m.Frees))
	return s.String()
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func PowerOn(ctx context.Context, wp *sync.WaitGroup, wgEnd *sync.WaitGroup) {
	defer wgEnd.Done()

	var ec float64

	slog.Log(LogLabel, "Powering on...")
	wp.Done()

	lch := make(chan util.UID)

	for {

		select {

		case eSn := <-EdgeSnCh:

			var e []EdgeSn
			// store edge in a slice of edge batches. Why? so we can fee batches during the convert phase from internal IDs to UUIDs.
			// optionally: save entries to a file
			switch {
			case edges_.j == 0 && edges_.k == 0:
				e = make([]EdgeSn, eBatchSize, eBatchSize)
				edges_.store = append(edges_.store, e)

			case edges_.k == eBatchSize:
				e = make([]EdgeSn, eBatchSize, eBatchSize)
				edges_.k = 0
				edges_.j++
				edges_.store = append(edges_.store, e)

			default:
				e = edges_.store[edges_.j]
			}
			e[edges_.k] = eSn
			edges_.k++

			ec++
			if math.Mod(ec, 100) == 0 {
				slog.Log(LogLabel, fmt.Sprintf("Edge Count. %g", ec))
			}

		case <-JoinNodes:

			//attachDone = make(attachDoneMap)
			attachRunning = make(attachRunningMap)
			var (
				dontrun bool
				eKey    edgeKey
				ok      bool
			)
			//
			// convert from internal identifiers to UUIDs.
			//
			var (
				limit int
				e     EdgeSn
			)
			t0 := time.Now()
			slog.Log(LogLabel, printMemUsage())
			for j, es := range edges_.store {

				if j == len(edges_.store)-1 {
					limit = edges_.k
				} else {
					limit = eBatchSize
				}
				for k := 0; k < limit; k++ {
					e = es[k]
					// convert from internal IDs to UUIDs
					uuid.ReqCh <- uuid.Request{SName: e.CSn, RespCh: lch}
					csn := <-lch
					uuid.ReqCh <- uuid.Request{SName: e.PSn, RespCh: lch}
					psn := <-lch

					euid := Edge{Cuid: csn, Puid: psn, Sortk: e.Sortk}
					edges = append(edges, &euid)
				}
				edges_.store[j] = nil // free memory
				if math.Mod(float64(j), eGCfreq) == 0 {
					runtime.GC()
					slog.Log(LogLabel, printMemUsage())
				}
				// time.Sleep(50 * time.Millisecond) - GC doesn't appear to run while in sleep mode
			}
			edges_.store = nil
			t1 := time.Now()
			slog.Log(LogLabel, fmt.Sprintf("Edge internal ID to UUID conversion. Edges %d. Duration: %s", len(edges), t1.Sub(t0)))
			slog.Log(LogLabel, printMemUsage())

			if len(edges) > 0 {

				for attachDone < len(edges) {
					//
					for _, e := range edges {

						// ignore processed or processing edges in the case of multiple scans through edges
						if e.attached {
							continue
						}
						eKey = edgeKey{e.Cuid.String(), e.Puid.String(), e.Sortk}
						slog.Log(LogLabel, fmt.Sprintf("eKey: %#v\n", eKey))
						if _, ok = attachRunning[eKey]; ok {
							continue
						}
						//
						// poll for attachDone message
						//
						select {
						case e := <-attachDoneCh:
							slog.Log(LogLabel, fmt.Sprintf("** Received on attachDoneCh.... %#v", *e))
							attachDone++
							e.attached = true
							eKey := edgeKey{e.Cuid.String(), e.Puid.String(), e.Sortk}
							delete(attachRunning, eKey)
						default:
						}

						//
						// detect for possible concurrency issues with running attachers - for this to work we need to be aware of when attachers have finished (ie. done)
						//
						dontrun = false
						slog.Log(LogLabel, fmt.Sprintf("len(attachRunning) : %d\n", len(attachRunning)))
						for r, _ := range attachRunning {
							// slog.Log(LogLabel, fmt.Sprintf("AttachRunning....%s %s %s", r.CuidS, r.PuidS, r.Sortk))
							// if new edge shares any edges with currently running attach jobs move onto next edge
							e := eKey
							if e.CuidS == r.CuidS || e.PuidS == r.CuidS || e.CuidS == r.PuidS || e.PuidS == r.PuidS {
								dontrun = true
								slog.Log(LogLabel, "AttachRunning....BREAK..")
								break
							}
						}
						if dontrun {

							if attachDone == len(edges)-1 {
								slog.Log(LogLabel, fmt.Sprintf("sleep "))
								time.Sleep(50 * time.Millisecond)
								break
							}
							continue
						}
						// send AttachNode msg. This will be throttled by rdf.Loader's limiterAttach, allowed concurrent client.AttachNode routines

						slog.Log(LogLabel, fmt.Sprintf("AttachNode <- e %s, %s\n", e.Cuid, e.Sortk))
						AttachNodeCh <- e //Edge{Cuid: csn, Puid: psn, Sortk: e.Sortk} // attached: E: e}
						attachRunning[eKey] = true

					}
					//
					// wait for running attachers to complete before trying to attach nodes that "donotrun"
					//
					for i := len(attachRunning); i > 0; i-- {
						e := <-attachDoneCh
						slog.Log(LogLabel, fmt.Sprintf("**** received on attachDoneCh.... %#v", *e))
						attachDone++
						e.attached = true
						eKey := edgeKey{e.Cuid.String(), e.Puid.String(), e.Sortk}
						delete(attachRunning, eKey)
						slog.Log(LogLabel, fmt.Sprintf("attachDone: %d  len(edges): %d\n", attachDone, len(edges)))
					}
				}
				edges = nil
				attachRunning = nil
			}
			// all edges joined - send end-of-data on channel
			slog.Log(LogLabel, fmt.Sprintf("attachDone: %d  len(edges): %d. Terminate..\n", attachDone, len(edges)))
			AttachNodeCh <- &Edge{Cuid: []byte("eod")}

		case <-ctx.Done():
			slog.Log("anmgr: ", "Powering down...")
			return

		}
	}
}
