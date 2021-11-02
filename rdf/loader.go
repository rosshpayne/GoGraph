package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	blk "github.com/GoGraph/block"
	//"github.com/GoGraph/client"
	//"github.com/GoGraph/db"
	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/gql/monitor"
	"github.com/GoGraph/run"
	"github.com/GoGraph/tbl"
	//"github.com/GoGraph/rdf/anmgr"
	//"github.com/GoGraph/rdf/dp"
	elog "github.com/GoGraph/errlog"
	"github.com/GoGraph/grmgr"
	"github.com/GoGraph/rdf/ds"
	"github.com/GoGraph/rdf/reader"
	"github.com/GoGraph/rdf/save"
	"github.com/GoGraph/rdf/uuid"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/types"
	"github.com/GoGraph/util"
)

const (
	// number of nodes in rdf to load in single read: TODO should be number of tuples not nodes as a node may contain
	// millions of rows - not a scalable and work load balanced design when based on nodes

	logid = param.Logid
)
const (
	I  = "I"
	F  = "F"
	S  = "S"
	Nd = "Nd"
	// SS  = ""SS
	// SI  = "SI"
	// SF  = "SF"
	LS  = "LS"
	LI  = "LI"
	LF  = "LF"
	LBl = "LbL"
	//SBl = "SBl"
)

//
type savePayload struct {
	sname        string   // node ID aka ShortName or blank-node-id
	suppliedUUID util.UID // user supplied UUID
	attributes   []ds.NV
}

//
// channels
//
var verifyCh chan verifyNd
var saveCh chan savePayload //[]ds.NV // TODO: consider using a struct {SName, UUID, []ds.NV}

//
//
var errNodes ds.ErrNodes

type verifyNd struct {
	n     int
	nodes []*ds.Node
}

func syslog(s string) {
	slog.Log(logid, s)
}

func init() {
	errNodes = make(ds.ErrNodes)
}

var inputFile = flag.String("f", "rdf_test.rdf", "RDF Filename: ")
var debug = flag.Int("debug", 0, "Enable full logging [ 1: enable] 0: disable")
var concurrent = flag.Int("c", 6, "# parallel goroutines")
var graph = flag.String("g", "", "Graph: ")
var showsql = flag.Int("sql", 0, "Show generated SQL [1: enable 0: disable]")
var reduceLog = flag.Int("rlog", 1, "Reduced Logging [1: enable 0: disable]")

// var inputFile = flag.String("f", "rdf_test.rdf", "RDF Filename: ")
// var graph = flag.String("g", "", "Graph: ")
// var concurrent = flag.Int("c", 6, "concurrent goroutines: ")
// var showsql = flag.Bool("sql", false, "ShowSQL: ")
// var debug = flag.Bool("debug", false, "Debug: ")

// uid PKey of the sname-UID pairs - consumed and populated by the SaveRDFNode()

func main() { //(f io.Reader) error { // S P O
	//
	flag.Parse()
	//
	syslog(fmt.Sprintf("Argument: inputfile: %s", *inputFile))
	// syslog(fmt.Sprintf("Argument: graph: %s", *graph))
	// //syslog(fmt.Sprintf("Argument: Node Attachers: %d", *attachers))
	// syslog(fmt.Sprintf("Argument: concurrent: %d", *concurrent))
	// syslog(fmt.Sprintf("Argument: showsql: %v", *showsql))
	// syslog(fmt.Sprintf("Argument: debug: %v", *debug))

	syslog(fmt.Sprintf("Argument: concurrency: %d", *concurrent))
	syslog(fmt.Sprintf("Argument: showsql: %v", *showsql))
	syslog(fmt.Sprintf("Argument: debug: %v", *debug))
	syslog(fmt.Sprintf("Argument: graph: %s", *graph))
	syslog(fmt.Sprintf("Argument: reduced logging: %v", *reduceLog))

	fmt.Printf("Argument: concurrent: %d\n", *concurrent)
	fmt.Printf("Argument: showsql: %v\n", *showsql)
	fmt.Printf("Argument: debug: %v\n", *debug)
	fmt.Printf("Argument: graph: %s\n", *graph)
	fmt.Printf("Argument: reduced logging: %v\n", *reduceLog)
	//
	// initialise channels with buffers
	verifyCh = make(chan verifyNd, 3)
	saveCh = make(chan savePayload, 12)
	//
	// if *showsql {
	// 	param.ShowSQL = true
	// }
	// if *debug {
	// 	param.DebugOn = true
	// }
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
	readBatchSize := *concurrent //  keep as same size as concurrent argument
	flag.PrintDefaults()
	//
	// set graph to use
	//
	if len(*graph) == 0 {
		fmt.Printf("Must supply a graph name\n")
		flag.PrintDefaults()
		return
	}

	err := types.SetGraph(*graph)
	if err != nil {
		syslog(fmt.Sprintf("Error in SetGraph: %s ", err.Error()))
		fmt.Printf("Error in SetGraph: %s\n", err)
		return
	}
	//
	f, err := os.Open(*inputFile)
	if err != nil {
		syslog(fmt.Sprintf("Error opening file %q, %s", *inputFile, err))
		fmt.Println(err)
		return
	}
	//
	// context - used to shutdown goroutines that are not part fo the pipeline
	//
	ctx, cancel := context.WithCancel(context.Background())
	//
	var (
		wpStart, wpEnd sync.WaitGroup
		ctxEnd         sync.WaitGroup
		n              int // for loop counter
		eof            bool
		runid          int64
	)
	// allocate a run id
	runid, err = run.New(logid, "rdfLoader")
	if err != nil {
		fmt.Println(fmt.Sprintf("Error in  MakeRunId() : %s", err))
		return
	}
	defer run.Finish(err)
	tstart := time.Now()
	//
	wpStart.Add(6) //7)
	// check verify and saveNode have finished. Each goroutine is responsible for closing and waiting for all routines they spawn.
	wpEnd.Add(2)
	// services
	ctxEnd.Add(4) //5)
	//
	// start pipeline goroutines
	//
	go verify(&wpStart, &wpEnd)
	go saveNode(&wpStart, &wpEnd)
	//
	// start supporting services
	//
	go uuid.PowerOn(ctx, &wpStart, &ctxEnd)         // generate and store UUIDs service
	go grmgr.PowerOn(ctx, &wpStart, &ctxEnd, runid) // concurrent goroutine manager service
	go elog.PowerOn(ctx, &wpStart, &ctxEnd)         // error logging service
	//go anmgr.PowerOn(ctx, &wpStart, &ctxEnd)   // attach node service
	go monitor.PowerOn(ctx, &wpStart, &ctxEnd) // repository of system statistics service
	//	go es.PowerOn(ctx, &wpStart, &ctxEnd)      // elasticsearch indexer
	//
	// wait for processes to start
	//
	syslog(fmt.Sprintf("waiting on services to start...."))
	wpStart.Wait()
	syslog(fmt.Sprintf("all load services started "))
	//
	// create rdf reader
	//
	rdr, _ := reader.New(f)

	for {
		//
		// make nodes
		//
		nodes := make([]*ds.Node, readBatchSize, readBatchSize)
		// assign pointers
		for i := range nodes {
			nodes[i] = new(ds.Node)
		}
		//
		// read rdf file []nodes at a time
		//
		n, eof, err = rdr.Read(nodes)
		if err != nil {
			// log error and continue to read until eof reached
			elog.Add(logid, fmt.Errorf("Read error: %s", err.Error()))
		}
		//
		// send []nodes on pipeline to be unmarshalled and saved to db
		//
		v := verifyNd{n: n, nodes: nodes}

		verifyCh <- v
		//
		// exit when
		if n < len(nodes) || eof {
			break
		}
	}
	//
	// shutdown verify and save routines
	//
	close(verifyCh)
	//
	wpEnd.Wait()
	//
	// errors
	printErrors()
	//
	// shutdown support services
	cancel()
	time.Sleep(1 * time.Second)
	ctxEnd.Wait()
	tend := time.Now()
	syslog(fmt.Sprintf("Exit.....Duration: %s", tend.Sub(tstart).String()))
	return
}

func printErrors() {

	elog.ReqErrCh <- struct{}{}
	errs := <-elog.RequestCh
	syslog(fmt.Sprintf(" ==================== ERRORS : %d	==============", len(errs)))
	fmt.Printf(" ==================== ERRORS : %d	==============\n", len(errs))
	if len(errs) > 0 {
		for _, e := range errs {
			syslog(fmt.Sprintf(" %s:  %s", e.Id, e.Err))
			fmt.Println(e.Id, e.Err)
		}
	}
}

// verify is a goroutine started when program is started.
// It persists for duration of load into database after which the verify channel from which it reads is closed.
// It forms part of a pipeline wiht the reader and main routine.
func verify(wpStart *sync.WaitGroup, wpEnd *sync.WaitGroup) { //, wg *sync.WaitGroup) {

	defer wpEnd.Done()
	defer close(saveCh)
	// sync verify's internal goroutines
	syslog("verify started....")
	wpStart.Done()

	// waitgroups
	var wg sync.WaitGroup
	//
	// concurrent settings for goroutines
	//
	//	unmarshalTrg := grmgr.Trigger{R: routine, C: 5, Ch: make(chan struct{})}

	limitUnmarshaler := grmgr.New("unmarshaler", *concurrent*2)

	// the loop will terminate on close of channel when rdf file is fully read.
	for nodes_ := range verifyCh {

		nodes := nodes_.nodes

		// unmarshal (& validate) each node in its own goroutine
		for i := 0; i < nodes_.n; i++ {

			if len(nodes[i].Lines) == 0 { // a line is a s-p-o tuple.//TODO: should i be ii
				break
			}
			ii := i
			ty, err := getType(nodes[ii])
			if err != nil {
				elog.Add(logid, err)
			}
			// first pipeline func. Passes NV data to saveCh and then to database.
			//	slog.Log("verify: ", fmt.Sprintf("Pass to unmarshal... %d %#v", i, nodes[ii]))
			limitUnmarshaler.Ask()
			<-limitUnmarshaler.RespCh()

			wg.Add(1)
			go unmarshalRDF(nodes[ii], ty, &wg, limitUnmarshaler)

		}
	}
	wg.Wait()
	limitUnmarshaler.Unregister()
}

//unmarshalRDF merges the rdf lines for an individual node (identical subject value) to create NV entries
// for the type of the node.
func unmarshalRDF(node *ds.Node, ty blk.TyAttrBlock, wg *sync.WaitGroup, lmtr *grmgr.Limiter) {
	defer wg.Done()

	genSortK := func(ty blk.TyAttrD) string {
		var s strings.Builder

		s.WriteString("A#") // leading sortk

		if ty.DT == "Nd" {
			// all uid-preds are listed under G partition
			s.WriteString("G#:")
			s.WriteString(ty.C)
		} else {
			s.WriteString(ty.P)
			s.WriteString("#:")
			s.WriteString(ty.C)
		}
		return s.String()
	}
	//
	defer lmtr.EndR()

	// accumulate predicate (spo) n.Object values in the following map
	type mergedRDF struct {
		value interface{}
		name  string // not populated below. TODO: why use it then.??
		dt    string
		sortk string
		c     string // type attribute short name
		ix    string // index type + support Has()
		null  bool   // true: nullable
	}
	// map[predicate]
	var attr map[string]*mergedRDF
	attr = make(map[string]*mergedRDF)
	//
	var nv []ds.NV // Node's AttributName-Value

	// find predicate in s-p-o lines matching pred  name in ty name
	// create attr entry indexed by pred.
	// may need to merge multiple s-p-o lines with the same pred into one attr entry e.g. list or set types
	// attr (predicate) will then be used to create NV entries, where the name (pred) gets associated with value (ob)
	var found bool

	for _, v := range ty {
		found = false
		//	fmt.Println("node.Lines: ", len(node.Lines), node.Lines)

		for _, n := range node.Lines {

			// match the rdf node pred value to the nodes type attribute
			if !strings.EqualFold(v.Name, n.Pred) {
				continue
			}
			found = true

			switch v.DT {
			case I:
				// check n.Object can be coverted to int

				i, err := strconv.Atoi(n.Obj)
				if err != nil {
					err := fmt.Errorf("expected Integer got %s ", n.Obj)
					node.Err = append(node.Err, err)
					continue
				}
				attr[v.Name] = &mergedRDF{value: int64(i), dt: v.DT, ix: v.Ix, null: v.N, c: v.C}

			case F:
				// check n.Object can be converted to float
				f, err := strconv.ParseFloat(n.Obj, 64)
				if err != nil {
					err := fmt.Errorf("error in ParseFloat64 on %s ", n.Obj)
					node.Err = append(node.Err, err)
					continue
				}
				attr[v.Name] = &mergedRDF{value: f, dt: v.DT, ix: v.Ix}
				//attr[v.Name] = n.Obj // keep float as string as Dynamodb transport it as string

			case S:
				// check n.Object can be converted to float

				//attr[v.Name] = n.Obj
				attr[v.Name] = &mergedRDF{value: n.Obj, dt: v.DT, ix: v.Ix, null: v.N, c: v.C}

			// case SS:

			// 	if a, ok := attr[v.Name]; !ok {
			// 		ss := make([]string, 1)
			// 		ss[0] = n.Obj
			// 		attr[v.Name] = &mergedRDF{value: ss, dt: v.DT, c: v.C, null: v.N}
			// 	} else {
			// 		if ss, ok := a.value.([]string); !ok {
			// 			err := fmt.Errorf("Conflict with SS type at line %d", n.N)
			// 			node.Err = append(node.Err, err)
			// 		} else {
			// 			// merge (append) obj value with existing attr (pred) value
			// 			syslog(fmt.Sprintf("Add to SS . [%s]", n.Obj))
			// 			ss = append(ss, n.Obj)
			// 			attr[v.Name].value = ss
			// 		}
			// 	}

			// case SI:

			// 	if a, ok := attr[v.Name]; !ok {

			// 		si := make([]int, 1)
			// 		i, err := strconv.Atoi(n.Obj)
			// 		if err != nil {
			// 			err := fmt.Errorf("expected Integer got %s", n.Obj)
			// 			node.Err = append(node.Err, err)
			// 			continue
			// 		}
			// 		si[0] = i
			// 		syslog(fmt.Sprintf("Add to SI . [%d]", i))
			// 		attr[v.Name] = &mergedRDF{value: si, dt: v.DT, c: v.C, null: v.N}

			// 	} else {

			// 		if si, ok := a.value.([]int); !ok {
			// 			err := fmt.Errorf("Conflict with SS type at line %d", n.N)
			// 			node.Err = append(node.Err, err)
			// 		} else {
			// 			i, err := strconv.Atoi(n.Obj)
			// 			if err != nil {
			// 				err := fmt.Errorf("expected Integer got %s", n.Obj)
			// 				node.Err = append(node.Err, err)
			// 				continue
			// 			}
			// 			// merge (append) obj value with existing attr (pred) value
			// 			syslog(fmt.Sprintf("Add to SI . [%d]", i))
			// 			si = append(si, i)
			// 			attr[v.Name].value = si
			// 		}
			// 	}

			// case SBl:
			// case SB:
			// case LBl:
			// case LB:

			case LS:
				if a, ok := attr[v.Name]; !ok {
					ls := make([]string, 1)
					ls[0] = n.Obj
					attr[v.Name] = &mergedRDF{value: ls, dt: v.DT, c: v.C, null: v.N}
					//	attr[v.Name] = ls
				} else {
					if ls, ok := a.value.([]string); !ok {
						err := fmt.Errorf("Conflict with SS type at line %d", n.N)
						node.Err = append(node.Err, err)
					} else {
						ls = append(ls, n.Obj)
						attr[v.Name].value = ls
					}
				}

			case LI:
				if a, ok := attr[v.Name]; !ok {
					li := make([]int64, 1)
					i, err := strconv.Atoi(n.Obj)
					if err != nil {
						err := fmt.Errorf("expected Integer got %s", n.Obj)
						node.Err = append(node.Err, err)
						continue
					}
					li[0] = int64(i) // n.Obj  int
					//attr[v.Name] = li
					attr[v.Name] = &mergedRDF{value: li, dt: v.DT, null: v.N, c: v.C}
				} else {
					if li, ok := a.value.([]int64); !ok {
						err := fmt.Errorf("Conflict with LI type at line %d", n.N)
						node.Err = append(node.Err, err)
					} else {
						i, err := strconv.Atoi(n.Obj)
						if err != nil {
							err := fmt.Errorf("expected Integer got  %s", n.Obj)
							node.Err = append(node.Err, err)
							continue
						}
						li = append(li, int64(i))
						attr[v.Name].value = li
					}
				}

			case Nd:
				// _:d Friends _:abc .
				// _:d Friends _:b .
				// _:d Friends _:c .
				// need to convert n.Obj value of SName to UID
				if a, ok := attr[v.Name]; !ok {
					ss := make([]string, 1)
					ss[0] = n.Obj // child node
					attr[v.Name] = &mergedRDF{value: ss, dt: v.DT, c: v.C}
				} else {
					// attach child (obj) short name to value slice (reperesenting list of child nodes to be attached)
					if nd, ok := a.value.([]string); !ok {
						err := fmt.Errorf("Conflict with Nd type at line %d", n.N)
						node.Err = append(node.Err, err)
					} else {
						nd = append(nd, n.Obj)
						attr[v.Name].value = nd // child nodes: _:abc,_:b,_:c
					}
				}
				//	addEdgesCh<-
			}
			//
			// generate sortk key
			//
			at := attr[v.Name]
			at.sortk = genSortK(v)
		}
		//
		//
		//
		if !found {
			if !v.N && v.DT != "Nd" {
				err := fmt.Errorf("Not null type attribute %q must be specified in node %s", v.Name, node.ID)
				node.Err = append(node.Err, err)
			}
		}
		if len(node.Err) > 0 {
			slog.Log("unmarshalRDF: ", fmt.Sprintf("return with %d errors. First error:  %s", len(node.Err), node.Err[0].Error()))
			for _, e := range node.Err {
				elog.Add("unmarshall:", e)
			}
			//elog.AddBatch <- node.Err
			return
		}

	}
	//
	// unmarshal attr into NV -except Nd types, handle in next for
	//
	// add type of node to NV - note a A#A# means it is associated with the scalar attributes. If the node has no scalars it will always
	// have a A#A#T so the type of the node can be determined if only the scalar data is fetched.
	//
	e := ds.NV{Sortk: "A#A#T", SName: node.ID, Value: node.TyName, DT: "ty"}
	nv = append(nv, e)
	//
	// add scalar predicates
	//
	for k, v := range attr {
		//
		if v.dt == Nd {
			continue
		}
		//
		// for nullable attributes only, populate Ty (which should be anyway) plus Ix (with "x") so a GSI entry is created in Ty_Ix to support Has(<predicate>) func.
		//
		e := ds.NV{Sortk: v.sortk, Name: k, SName: node.ID, Value: v.value, DT: v.dt, C: v.c, Ty: node.TyName, Ix: v.ix}
		nv = append(nv, e)
	}
	//
	// check all uid-predicate types (DT="Nd") have an NV entry - as this simplies later processing if one is guaranteed to exist even if not originally defined in RDF file
	//
	for _, v := range ty {
		if v.DT == Nd {
			// create empty item
			value := []string{"__"}
			e := ds.NV{Sortk: genSortK(v), Name: v.Name, SName: "__", Value: value, DT: Nd, Ty: node.TyName} // TODO: added Ty so A#T item can be removed (at some point)
			nv = append(nv, e)
		}
	}
	//
	//  build list of attach node pairs (using anmgr) to be processed after all other node and predicates  have been added to db
	//
	lch := make(chan util.UID)
	var err error
	for _, v := range attr {
		if v.dt == "Nd" {
			// in the case of nodes wihtout scalars we need to add a type item
			x := v.value.([]string) // child nodes
			// for the node create a edge entry to each child node (for the Nd pred) in the anmgr service
			// These entries will be used later to attach the actual nodes together (propagate child data etc)
			etx := tx.New("edge")
			// get or generate node (parent) UID
			uuid.ReqCh <- uuid.Request{SName: node.ID, RespCh: lch}
			psn := <-lch

			etx.Add(mut.NewMutation(tbl.Edge_, psn, "", mut.Merge).AddMember("Cnt", len(x), mut.Inc))
			etx.MakeBatch()
			// save to db when child nodes reach batchLimit
			batchLimit_ := param.ChildBatch
			batchLimit := batchLimit_

			// for each child node
			for i, s := range x {

				uuid.ReqCh <- uuid.Request{SName: s, RespCh: lch}
				csn := <-lch

				// bulk insert should be in dedicated transaction or transaction Batch - see MakeBatch() above
				etx.Add(mut.NewBulkInsert(tbl.EdgeChild_).AddMember("Puid", psn).AddMember("Cuid", csn).AddMember("Sortk", v.sortk).AddMember("Status", "X"))

				if i == batchLimit {

					err := etx.MakeBatch()
					if err != nil {
						elog.Add(logid, err)
					}
					batchLimit += batchLimit_
				}
			}

			err = etx.Execute()

			if err != nil {
				elog.Add(logid, err)
			}

		}
	}
	//
	// pass NV onto save-to-database channel if no errors detected
	//
	if len(node.Err) == 0 {

		if len(nv) == 0 {
			panic(fmt.Errorf("unmarshalRDF: nv is nil "))
		}
		payload := savePayload{sname: node.ID, suppliedUUID: node.UUID, attributes: nv}

		// save nodes NV content to database
		saveCh <- payload

	} else {

		node.Lines = nil
		errNodes[node.ID] = node

	}
	//
}

func saveNode(wpStart *sync.WaitGroup, wpEnd *sync.WaitGroup) {

	defer wpEnd.Done()
	wpStart.Done()

	syslog("saveNode started......")
	var wg sync.WaitGroup
	//
	// define goroutine limiters
	//
	limiterSave := grmgr.New("saveNode", *concurrent)
	limiterES := grmgr.New("ES", *concurrent*param.ESgrMultipler)

	var c int

	// read from save channel. Payload represens an individual nodes NV data.
	for py := range saveCh {
		c++

		limiterSave.Ask()
		<-limiterSave.RespCh()

		wg.Add(1)
		go save.SaveRDFNode(py.sname, py.suppliedUUID, py.attributes, &wg, limiterSave, limiterES)
		//save.SaveRDFNode(py.sname, py.suppliedUUID, py.attributes, &wg, limiterSave, limiterES)

	}
	syslog(fmt.Sprintf("waiting for SaveRDFNodes to Unregister..... %d", c))
	wg.Wait()
	limiterSave.Unregister()
	limiterES.Unregister()
	syslog("saveNode finished waiting.....now to attach nodes")
	//
	// limiterAttach := grmgr.New("nodeAttach", *attachers)
	//
	// fetch edge node ids from attach-node-manager routine. This will send each edge node pair via its AttachNodeCh.
	//
	//anmgr.JoinNodes <- struct{}{}
	return
}

// }
// 	c = 0

// 	//AttachNodeCh is populated by service anmgr (AttachNodeManaGeR)

// 	for e := range anmgr.AttachNodeCh {
// 		c++
// 		//	e := <-anmgr.AttachNodeCh
// 		if string(e.Cuid) == "eod" {
// 			break
// 		}
// 		limiterAttach.Ask()
// 		<-limiterAttach.RespCh()

// 		wg.Add(1)

// 		go client.AttachNode(util.UID(e.Cuid), util.UID(e.Puid), e.Sortk, e, &wg, limiterAttach)
// 		//client.AttachNode(util.UID(e.Cuid), util.UID(e.Puid), e.Sortk, e, &wg, limiterAttach)

// 	}
// 	wg.Wait()
// 	limiterAttach.Finish()
// 	syslog("attach nodes finished waiting...")
// 	nb, err := db.Fetch("EOPCount")
// 	if err != nil {
// 		syslog(fmt.Sprintf("db.Fetch EOPCount: %s", err))
// 	}
// 	syslog(fmt.Sprintf("EOP Count: %d", nb[0].GetI()))
// 	// determine types which reference types that have a cardinality of 1:1

// 	has11 := make(map[string]struct{})
// 	dpTy := make(map[string]struct{})

// 	for k, v := range types.TypeC.TyC {
// 		for _, vv := range v {
// 			if vv.Ty == "" {
// 				continue
// 			}
// 			if _, ok := has11[k]; ok {
// 				break
// 			}
// if vv.Card == "1:1" {
//     has11[k] = struct{}{}
// }
// 		}
// 	}
// 	for k, v := range types.TypeC.TyC {
// 		for _, vv := range v {
// 			if _, ok := has11[vv.Ty]; ok {
// 				if sn, ok := types.GetTyShortNm(k); ok {
// 					dpTy[sn] = struct{}{}
// 				}
// 			}
// 		}
// 	}
// 	var wgc sync.WaitGroup
// 	limiterDP := grmgr.New("dp", *attachers)
// 	for k, _ := range dpTy {
// 		syslog(fmt.Sprintf(" Type containing 1:1 type: %s", k))
// 	}
// 	syslog("Start double propagation processing...")
// 	t0 := time.Now()
// 	for ty, _ := range dpTy {

// 		ty := ty
// 		for n := range dp.FetchNodeCh(ty) {

// 			n := n
// 			limiterDP.Ask()
// 			<-limiterDP.RespCh()
// 			wgc.Add(1)

// 			go dp.Process(limiterDP, &wgc, n, ty, has11)

// 		}
// 		wgc.Wait()
// 	}
// 	t1 := time.Now()
// 	limiterDP.Finish()
// 	syslog(fmt.Sprintf("double propagate processing finished. Duration: %s", t1.Sub(t0)))

// } 				has11[k] = struct{}{}

func getType(node *ds.Node) (blk.TyAttrBlock, error) {

	// type loc struct {
	// 	sync.Mutex
	// }
	//	var ll loc

	// is there a type defined
	if len(node.TyName) == 0 {
		node.Err = append(node.Err, fmt.Errorf("No type defined for %s", node.ID))
	}
	//syslog(fmt.Sprintf("node.TyName : [%s]", node.TyName))
	//ll.Lock() - all types loaded at startup time - no locks required
	//ty, err := cache.FetchType(node.TyName)
	ty, err := types.FetchType(node.TyName)
	//ll.Unlock()
	if err != nil {
		return nil, err
	}
	return ty, nil
}
