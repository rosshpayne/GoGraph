package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	param "github.com/GoGraph/dygparam"
	elog "github.com/GoGraph/errlog"
	"github.com/GoGraph/es/internal/db"
	"github.com/GoGraph/grmgr"
	"github.com/GoGraph/run"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/types"
	"github.com/GoGraph/util"

	esv7 "github.com/elastic/go-elasticsearch/v7"
	esapi "github.com/elastic/go-elasticsearch/v7/esapi"
	//	esv8 "github.com/elastic/go-elasticsearch/v8"
)

const (
	logid = "ElasticSearch: "
	esdoc = "gograph"
)

type logEntry struct {
	d    *Doc // node containing es index type
	pkey []byte
	err  error
	s    chan struct{}
}

type Doc struct {
	Attr  string // <GraphShortName>|<AttrName> e.g. m|title
	Value string
	PKey  string
	SortK string // A#A#:N
	Type  string //
}

var debug = flag.Int("debug", 0, "Enable full logging [ 1: enable] 0: disable")
var parallel = flag.Int("c", 3, "# ES loaders")
var graph = flag.String("g", "", "Graph: ")
var showsql = flag.Int("sql", 0, "Show generated SQL [1: enable 0: disable]")
var reduceLog = flag.Int("rlog", 1, "Reduced Logging [1: enable 0: disable]")
var runId int64

var tstart time.Time

func logerr(e error, panic_ ...bool) {

	if len(panic_) > 0 && panic_[0] {
		slog.Log(logid, e.Error(), true)
		panic(e)
	}
	slog.Log(logid, e.Error())
}

func syslog(s string) {
	slog.Log(logid, s)
}

func GetRunid() int64 {
	return runId
}

var (
	cfg esv7.Config
	es  *esv7.Client
	err error
)

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
		logCh          chan *logEntry
		saveCh         chan struct{} // execute logit tx
		saveAckCh      chan struct{}
	)

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
	//
	// establish connection to elasticsearch
	//
	err = connect()
	if err != nil {
		fmt.Println("Cannot establish connection to elasticsearch")
		return
	}
	//
	// create a run identifier
	//
	runid, err = run.New(logid, "dp")
	if err != nil {
		fmt.Println(fmt.Sprintf("Error in  MakeRunId() : %s", err))
		return
	}
	runId = runid
	syslog(fmt.Sprintf("runid: %d", runid))
	if runid < 1 {
		fmt.Printf("Abort: runid is invalid %d\n", runid)
		return
	}
	defer run.Finish(err)
	tstart = time.Now()
	// channels
	logCh = make(chan *logEntry)
	saveCh = make(chan struct{})
	saveAckCh = make(chan struct{})

	batch := db.NewBatch()

	ctx, cancel := context.WithCancel(context.Background())

	wpEnd.Add(3)
	wpStart.Add(3)

	// log es loads to a log - to enable restartability
	go logit(ctx, &wpStart, &wpEnd, logCh, saveCh, saveAckCh)
	// services
	//go stop.PowerOn(ctx, &wpStart, &wpEnd)    // detect a kill action (?) to terminate program alt just kill-9 it.
	go grmgr.PowerOn(ctx, &wpStart, &wpEnd, runid) // concurrent goroutine manager service
	go elog.PowerOn(ctx, &wpStart, &wpEnd)         // error logging service
	// Dynamodb only: go monitor.PowerOn(ctx, &wpStart, &wpEnd)      // repository of system statistics service
	wpStart.Wait()

	syslog("All services started. Proceed with attach processing")

	err = types.SetGraph(*graph)
	if err != nil {
		syslog(fmt.Sprintf("Error in SetGraph: %s ", err.Error()))
		fmt.Printf("Error in SetGraph: %s\n", err)
		return
	}

	type sortk = string
	type tySn = string

	esAttr := make(map[tySn]sortk)

	for k, v := range types.TypeC.TyC {
		for _, vv := range v {
			switch vv.Ix {
			case "FT", "ft":

				var sortk strings.Builder
				sortk.WriteString("A#")
				sortk.WriteString(vv.P)
				sortk.WriteString("#:")
				sortk.WriteString(vv.C)
				ty, _ := types.GetTyShortNm(k)

				esAttr[ty] = sortk.String()
			}
		}
	}
	syslog(fmt.Sprintf("esAttr: %#v", esAttr))

	lmtrES := grmgr.New("es", *parallel)
	var loadwg sync.WaitGroup

	for tysn, sk := range esAttr {

		go db.ScanForESentry(tysn, sk, batch, saveCh, saveAckCh)

		// retrieve records from nodescalar for FT fields (always an S type)
		for {
			for r := range batch.FetchCh {

				// S contains the value "<typeShortName>:<value>"

				doc := &Doc{Attr: r.IxValue[strings.Index(r.IxValue, ".")+1:], Value: r.Value, PKey: util.UID(r.PKey).ToString(), SortK: esAttr[r.Ty], Type: r.Ty}

				lmtrES.Ask()
				<-lmtrES.RespCh()
				loadwg.Add(1)

				go load(doc, r.PKey, &loadwg, lmtrES, logCh)
			}
			// batch data has been modified by db package - take a copy
			batch = <-batch.BatchCh

			// wait for es load groutines to finish
			loadwg.Wait()
			batch.LoadAckCh <- struct{}{}

			if batch.Eod {
				break
			}
		}
	}
	loadwg.Wait()
	//
	// errors
	printErrors()
	//
	// shutdown support services
	cancel()
	time.Sleep(1 * time.Second)
	wpEnd.Wait()
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

func connect() error {

	cfg = esv7.Config{
		Addresses: []string{
			//"http://ec2-54-234-180-49.compute-1.amazonaws.com:9200",
			"http://instance-1:9200",
		},
		// ...
	}
	es, err = esv7.NewClient(cfg)
	if err != nil {
		elog.Add(logid, fmt.Errorf("ES Error creating the client: %s", err))
		return err
	}
	//
	// 1. Get cluster info
	//
	res, err := es.Info()
	if err != nil {
		elog.Add(logid, fmt.Errorf("ES Error getting Info response: %s", err))
		return err
	}
	defer res.Body.Close()
	// Check response status
	if res.IsError() {
		elog.Add(logid, fmt.Errorf("ES Error: %s", res.String()))
		return err
	}
	// Deserialize the response into a map.
	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		elog.Add(logid, fmt.Errorf("ES Error parsing the response body: %s", err))
		return err
	}
	// Print client and server version numbers.
	syslog(fmt.Sprintf("Client: %s", esv7.Version))
	syslog(fmt.Sprintf("Server: %s", r["version"].(map[string]interface{})["number"]))

	return nil
}

func logit(ctx context.Context, wpStart *sync.WaitGroup, wpEnd *sync.WaitGroup, logCh <-chan *logEntry, saveCh <-chan struct{}, saveAckCh chan<- struct{}) {

	wpStart.Done()
	defer wpEnd.Done()

	var ltx *tx.Handle
	slog.Log("logit: ", "starting up...")

	for {

		select {
		case es := <-logCh:

			if ltx == nil {
				ltx = tx.New("logit")
			}
			ltx.Add(ltx.NewBulkInsert(tbl.Eslog).AddMember("PKey", es.pkey).AddMember("runid", GetRunid()).AddMember("Sortk", es.d.Attr).AddMember("Ty", es.d.Type).AddMember("Graph", types.GraphSN()))

			es.s <- struct{}{}

		case <-saveCh:

			err := ltx.Execute()
			if err != nil {
				elog.Add("logit", err)
			}
			ltx = tx.New("logit")
			// acknowledge log data is saved
			saveAckCh <- struct{}{}

		case <-ctx.Done():
			slog.Log("logit: ", "shutdown...")
			return
		}
	}

}
func load(d *Doc, pkey []byte, wp *sync.WaitGroup, lmtr *grmgr.Limiter, logCh chan<- *logEntry) {

	defer lmtr.EndR()
	defer wp.Done()

	// Initialize a client with the default settings.
	//
	//	es, err := esv7.NewClient(cfg)
	// if err != nil {
	// 	syslog(fmt.Sprintf("Error creating the client: %s", err))
	// }
	//
	// 2. Index document
	//
	// Build the request body.

	var b, t, doc strings.Builder
	b.WriteString(`{"attr" : "`)
	b.WriteString(d.Attr)
	b.WriteString(`","value" : "`)
	b.WriteString(d.Value)
	b.WriteString(`","sortk" : "`)
	b.WriteString(d.SortK)
	b.WriteString(`","type" : "`)
	//type must be associated with the graph before its stored in elasticsearch <graphshortname>.<typeShortName>
	t.WriteString(types.GraphSN())
	t.WriteByte('.')
	t.WriteString(d.Type)
	b.WriteString(t.String())
	b.WriteString(`"}`)
	//
	doc.WriteString(d.PKey)
	doc.WriteByte('|')
	doc.WriteString(d.Attr)
	//
	syslog(fmt.Sprintf("Body: %s   Doc: %s", b.String(), doc.String()))
	// Set up the request object.
	t0 := time.Now()
	req := esapi.IndexRequest{
		Index:      param.ESindex,
		DocumentID: doc.String(),
		Body:       strings.NewReader(b.String()),
		Refresh:    "true",
	}

	// Perform the request with the client.
	res, err := req.Do(context.Background(), es)
	t1 := time.Now()
	if err != nil {
		elog.Add(logid, fmt.Errorf("Error getting response: %s", err))
	}
	defer res.Body.Close()

	if res.IsError() {
		elog.Add(logid, fmt.Errorf("Error indexing document ID=%s. Status: %v ", d.PKey, res.Status()))
	} else {
		// Deserialize the response into a map.
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			elog.Add(logid, fmt.Errorf("Error parsing the response body: %s", err))
		} else {
			// Print the response status and indexed document version.
			syslog(fmt.Sprintf("[%s] %s; version=%d   API Duration: %s", res.Status(), r["result"].(string), int(r["_version"].(float64)), t1.Sub(t0)))
		}
	}
	s := make(chan struct{})
	logCh <- &logEntry{d, pkey, err, s}
	<-s

}

//
// 3. Search for the indexed documents
//
// Build the request body.
// var buf bytes.Buffer
// query := map[string]interface{}{
// 	"query": map[string]interface{}{
// 		"match": map[string]interface{}{
// 			"text": "test",
// 		},
// 	},
// }
// if err := json.NewEncoder(&buf).Encode(query); err != nil {
// 	syslog(fmt.Sprintf("Error encoding query: %s", err))
// }

// // Perform the search request.
// res, err = es.Search(
// 	es.Search.WithContext(context.Background()),
// 	es.Search.WithIndex("graphstrings"),
// 	es.Search.WithBody(&buf),
// 	es.Search.WithTrackTotalHits(true),
// 	es.Search.WithPretty(),
// )
// if err != nil {
// 	syslog(fmt.Sprintf("Error getting response: %s", err))
// }
// defer res.Body.Close()

// if res.IsError() {
// 	var e map[string]interface{}
// 	if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
// 		syslog(fmt.Sprintf("Error parsing the response body: %s", err))
// 	} else {
// 		// Print the response status and error information.
// 		lsyslog(fmt.Sprintf("[%s] %s: %s",
// 			res.Status(),
// 			e["error"].(map[string]interface{})["type"],
// 			e["error"].(map[string]interface{})["reason"],
// 		))
// 	}
// }

// if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
// 	syslog(fmt.Sprintf("Error parsing the response body: %s", err))
// }
// // Print the response status, number of results, and request duration.
// syslog(fmt.Sprintf(
// 	"[%s] %d hits; took: %dms",
// 	res.Status(),
// 	int(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64)),
// 	int(r["took"].(float64)),
// ))
// // Print the ID and document source for each hit.
// for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
// 	log.Printf(" * ID=%s, %s", hit.(map[string]interface{})["_id"], hit.(map[string]interface{})["_source"])
// }

// log.Println(strings.Repeat("=", 37))
