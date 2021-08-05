package db

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	blk "github.com/GoGraph/block"
	"github.com/GoGraph/dbConn"
	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/rdf/ds"
	"github.com/GoGraph/rdf/es"
	"github.com/GoGraph/rdf/grmgr"
	"github.com/GoGraph/rdf/uuid"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/types"
	"github.com/GoGraph/util"

	"github.com/GoGraph/db"
)

const (
	logid = "rdfSaveDB: "
)

type tyNames struct {
	ShortNm string `json:"Atr"`
	LongNm  string
}

var (
	dynSrv    *dynamodb.DynamoDB
	err       error
	tynames   []tyNames
	tyShortNm map[string]string
)

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

func init() {
	dynSrv = dbConn.New()
}

//TODO: this routine requires an error log service. Code below  writes errors to the screen in some cases but not most. Errors are returned but calling routines is a goroutine so thqt get lost.
// sname : node id, short name  aka blank-node-id
// uuid  : user supplied node id (util.UIDb64 converted to util.UID)
// nv_ : node attribute data
func saveRDFNode(sname string, suppliedUUID util.UID, nv_ []ds.NV, wg *sync.WaitGroup, lmtr *grmgr.Limiter, lmtrES *grmgr.Limiter) {

	defer wg.Done()
	defer func() func() {
		return func() {
			err := err
			if err != nil {
				syslog(fmt.Sprintf("Error: [%s]", err.Error()))
			} else {
				syslog(fmt.Sprintf("Finished"))
			}
		}
	}()()

	lmtr.StartR()
	defer lmtr.EndR()
	//
	// generate UUID using uuid service
	//
	localCh := make(chan util.UID, "NodeScalar")
	request := uuid.Request{SName: sname, SuppliedUUID: suppliedUUID, RespCh: localCh}

	uuid.ReqCh <- request

	UID := <-localCh

	var (
		//NdUid     util.UID
		tyShortNm string
	)

	txh := tx.New("SaveNode")

	for _, nv := range nv_ {

		mut := db.NewMutation(NodeScalarTbl, UID, nv.Sortk, tx.Insert)

		tyShortNm, _ = types.GetTyShortNm(nv.Ty)
		// if tyShortNm, ok = types.GetTyShortNm(nv.Ty); !ok {
		// 	syslog(fmt.Sprintf("Error: type name %q not found in types.GetTyShortNm \n", nv.Ty))
		// 	panic(fmt.Errorf("Error: type name %q not found in types.GetTyShortNm. sname: %s, nv: %#v\n", nv.Ty, sname, nv))
		// }
		///syslog(fmt.Sprintf("saveRDF: tySHortNm = %q", tyShortNm))

		//fmt.Printf("In saveRDFNode:  nv = %#v\n", nv)
		// append child attr value to parent uid-pred list
		switch nv.DT {

		case "I":
			// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
			if i, ok := nv.Value.(int64); !ok {
				panic(fmt.Errorf("Value is not an Int "))
			} else {
				//(sk string, m string, param string, value interface{}) {
				mut.AddMember("I", i)
				mut.AddMember("P", nv.Name)
				mut.AddMember("Ty", tyShortNm)
				//a := Item{PKey: UID, SortK: nv.Sortk, N: i, P: nv.Name, Ty: tyShortNm} // nv.Ty}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					panic(fmt.Errorf("%s: %s", "failed to marshal type definition ", err.Error()))
				}
			}

		case "F":

			// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
			if f, ok := nv.Value.(float64); ok {
				// populate with dummy item to establish LIST
				mut.AddMember("F", f)
				mut.AddMember("P", nv.Name)
				mut.AddMember("Ty", tyShortNm)
				//a := Item{PKey: UID, SortK: nv.Sortk, N: f, P: nv.Name, Ty: tyShortNm} //nv.Ty}
			} else {
				panic(fmt.Errorf(" nv.Value is not an string (float) for predicate  %s", nv.Name))
			}

		case "S":

			// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
			if v, ok := nv.Value.(string); ok {
				//
				// use Ix attribute to determine whether the P attribute (PK of GSI) should be populated.
				//  For Ix value of FT (full text search)the S attribute will not appear in the GSI (P_S) as ElasticSearch has it covered
				//  For Ix value of "x" is used for List or Set types which will have the result of expanding the array of values into
				//  individual items which will be indexed. Usual to be able to query contents of a Set/List.
				//  TODO: is it worthwhile have an FTGSI attribute to have it both index in ES and GSI
				//
				switch nv.Ix {

				case "FTg", "ftg":
					//
					// load item into ElasticSearch index
					//
					ea := &es.Doc{Attr: nv.Name, Value: v, PKey: UID.ToString(), SortK: nv.Sortk, Type: tyShortNm}

					//es.IndexCh <- ea
					lmtrES.Ask()
					<-lmtrES.RespCh()

					go es.Load(ea, lmtrES)

					// load into GSI by including attribute P in item
							mut.AddMember("P", nv.Name)
					mut.AddMember("S", v)
					mut.AddMember("Ty", tyShortNm)
					//a := Item{PKey: UID, SortK: nv.Sortk, S: v, P: nv.Name, Ty: tyShortNm} //nv.Ty}

				case "FT", "ft":

					// ea := &es.Doc{Attr: nv.Name, Value: v, PKey: UID.ToString(), SortK: nv.Sortk, Type: tyShortNm}

					// //es.IndexCh <- ea
					// //go es.Load(ea)
					// lmtrES.Ask()
					// <-lmtrES.RespCh()

					// go es.Load(ea, lmtrES)

					// don't load into GSI by eliminating attribute P from item. GSI use P as their PKey.
					mut.AddMember("P", nv.Name)
					mut.AddMember("S", v)
					mut.AddMember("Ty", tyShortNm)
					//a := Item{PKey: UID, SortK: nv.Sortk, S: v, Ty: tyShortNm} //nv.Ty}

				default:
					// load into GSI by including attribute P in item
					mut.AddMember("P", nv.Name)
					mut.AddMember("S", v)
					mut.AddMember("Ty", tyShortNm)
					//a := Item{PKey: UID, SortK: nv.Sortk, S: v, P: nv.Name, Ty: tyShortNm} //nv.Ty}

				}
			} else {
				panic(fmt.Errorf(" nv.Value is not an string "))
			}

		case "DT": // DateTime

			// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
			if dt, ok := nv.Value.(time.Time); ok {
				// populate with dummy item to establish LIST
				mut.AddMember("P", nv.Name)
				mut.AddMember("DT", dt.String())
				mut.AddMember("Ty", tyShortNm)
				//a := Item{PKey: UID, SortK: nv.Sortk, DT: dt.String(), P: nv.Name, Ty: tyShortNm} //nv.Ty}
			} else {
				panic(fmt.Errorf(" nv.Value is not an String "))
			}

		case "ty": // node type entry

			// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
			if s, ok := nv.Value.(string); ok {
				if tyShortNm, ok = types.GetTyShortNm(s); !ok {
					syslog(fmt.Sprintf("Error: type name %q not found in types.GetTyShortNm \n", nv.Ty))
					return
				}
				mut.AddMember("A#A#T", s)
				//a := Item{PKey: UID, SortK: "A#A#T", Ty: tyShortNm, Ix: "X"}
			} else {
				panic(fmt.Errorf(" nv.Value is not an string for attribute %s ", nv.Name))
			}

		case "Bl":

			if f, ok := nv.Value.(bool); ok {
				// populate with dummy item to establish LIST
				mut.AddMember("Bl", f)
				mut.AddMember("P", nv.Name)
				mut.AddMember("Ty" tyShortNm)
				//a := Item{PKey: UID, SortK: nv.Sortk, Bl: f, P: nv.Name, Ty: tyShortNm} //nv.Ty}
			} else {
				panic(fmt.Errorf(" nv.Value is not an BL for attribute %s ", nv.Name))
			}

		case "B":

			if f, ok := nv.Value.([]byte); ok {
				// populate with dummy item to establish LIST
				mut.AddMember("B", f)
				mut.AddMember("P", nv.Name)
				mut.AddMember("Ty", tyShortNm)
				//a := Item{PKey: UID, SortK: nv.Sortk, B: f, P: nv.Name, Ty: tyShortNm} //nv.Ty}
			} else {
				panic(fmt.Errorf(" nv.Value is not an []byte "))
			}

		case "LI":

			mut.SetOpr(tx.Merge)
			if i, ok := nv.Value.([]int64); ok {
				// populate with dummy item to establish LIST
				mut.AddMember("LI", i)
				mut.AddMember("Ty", tyShortNm)
				//a := Item{PKey: UID, SortK: nv.Sortk, LN: f, Ty: tyShortNm}
			} else {
				panic(fmt.Errorf(" nv.Value is not an []int64 for attribute, %s. Type: %T ", nv.Name, nv.Value))
			}

		case "LF":

			mut.SetOpr(tx.Merge)			
			if f, ok := nv.Value.([]float64); ok {
				// populate with dummy item to establish LIST
				mut.AddMember("LF", f)
				mut.AddMember("Ty", tyShortNm)
				//a := Item{PKey: UID, SortK: nv.Sortk, LN: f, Ty: tyShortNm}
			} else {
				panic(fmt.Errorf(" nv.Value is not an LF for attribute %s ", nv.Name))
			}

		case "LS":

			mut.SetOpr(tx.Merge)
			if s, ok := nv.Value.([]string); ok {
				// populate with dummy item to establish LIST
				mut.AddMember("LS", s)
				mut.AddMember("Ty", tyShortNm)
				//a := Item{PKey: UID, SortK: nv.Sortk, LN: f, Ty: tyShortNm}
			} else {
				panic(fmt.Errorf(" nv.Value is not an LF for attribute %s ", nv.Name))
			}

		ase "LDT":

			mut.SetOpr(tx.Merge)
			if dt, ok := nv.Value.([]string); ok {
				// populate with dummy item to establish LIST
				mut.AddMember("LDT", dt)
				mut.AddMember("Ty", tyShortNm)
				//a := Item{PKey: UID, SortK: nv.Sortk, LN: f, Ty: tyShortNm}
			} else {
				panic(fmt.Errorf(" nv.Value is not an LF for attribute %s ", nv.Name))
			}

			// TODO: others LBl, LB, 

		// case "SI":

		// 	if f, ok := nv.Value.([]int); ok {
		// 		// populate with dummy item to establish LIST
		// 		mut.AddMember(nv.SortK, f)
		// 		mut.AddMember(nv.SortK, tyShortNm)
		// 		//a := Item{PKey: UID, SortK: nv.Sortk, SN: f, Ty: tyShortNm}
		// 	} else {
		// 		panic(fmt.Errorf(" nv.Value is not a slice of SI for attribute %s ", nv.Name))
		// 	}

		// case "SF":

		// 	if f, ok := nv.Value.([]float64); ok {
		// 		// populate with dummy item to establish LIST
		// 		mut.AddMember(nv.SortK, f)
		// 		mut.AddMember(nv.SortK, tyShortNm)
		// 		//a := Item{PKey: UID, SortK: nv.Sortk, SN: f, Ty: tyShortNm}
		// 	} else {
		// 		panic(fmt.Errorf("SF nv.Value is not an slice float64 "))
		// 	}

		// case "SBl":

		// 	if f, ok := nv.Value.([]bool); ok {
		// 		// populate with dummy item to establish LIST
		// 		mut.AddMember(nv.SortK, f)
		// 		mut.AddMember(nv.SortK, tyShortNm)
		// 		//a := Item{PKey: UID, SortK: nv.Sortk, SBl: f, Ty: tyShortNm}
		// 	} else {
		// 		panic(fmt.Errorf("Sbl nv.Value is not an slice of bool "))
		// 	}

		// case "SS":

		// 	if f, ok := nv.Value.([]string); ok {
		// 		// populate with dummy item to establish LIST
		// 		mut.AddMember(nv.SortK, f)
		// 		mut.AddMember(nv.SortK, tyShortNm)
		// 		//a := Item{PKey: UID, SortK: nv.Sortk, SS: f, Ty: tyShortNm}
		// 	} else {
		// 		panic(fmt.Errorf(" SSnv.Value is not an String Set for attribte %s ", nv.Name))
		// 	}

		// case "SB":

		// 	if f, ok := nv.Value.([][]byte); ok {
		// 		// populate with dummy item to establish LIST
		// 		mut.AddMember(nv.SortK, f)
		// 		mut.AddMember(nv.SortK, tyShortNm)
		// 		//a := Item{PKey: UID, SortK: nv.Sortk, SB: f, Ty: tyShortNm}
		// 	} else {
		// 		panic(fmt.Errorf("SB nv.Value is not an Set of Binary for predicate %s ", nv.Name))
		// 	}

		case "Nd":

			mut.SetOpr(tx.Merge)
			// convert node blank name to UID
			xf := make([]int, 1, 1)
			xf[0] = blk.ChildUID
			id := make([]int, 1, 1)
			id[0] = 0
			if f, ok := nv.Value.([]string); ok {
				// populate with dummy item to establish LIST
				uid := make([][]byte, len(f), len(f))
				xf := make([]int, len(f), len(f))
				id := make([]int, len(f), len(f))
				for i, n := range f {
					request := uuid.Request{SName: n, RespCh: localCh}
					//syslog(fmt.Sprintf("UID Nd request  : %#v", request))

					uuid.ReqCh <- request

					UID := <-localCh

					uid[i] = []byte(UID)
					xf[i] = blk.ChildUID
					id[i] = 0

				}
				//NdUid = UID // save to use to create a Type item
				mut.AddMember("Nd", uid)
				mut.AddMember("XF", xf)
				mut.AddMember("Id", id)
				mut.AddMember("Ty", tyShortNm)
				//a := Item{PKey: UID, SortK: nv.Sortk, Nd: uid, XF: xf, Id: id, Ty: tyShortNm}
			} else {
				panic(fmt.Errorf(" Nd nv.Value is not an string slice "))
			}
		}
		//
		txh.Add(mut)
	}

	txh.Execute()

	//
	// expand Set and List types into individual S# entries to be indexed// TODO: what about SN, LN
	//
	// for _, nv := range nv_ {

	// 	input := db.NewInput()

	// 	// append child attr value to parent uid-pred list
	// 	switch nv.DT {

	// 	case "SS":

	// 		var sk string
	// 		if ss, ok := nv.Value.([]string); ok {
	// 			//
	// 			input := db.NewInput(UID)

	// 			for i, s := range ss {

	// 				if tyShortNm, ok = types.GetTyShortNm(nv.Ty); !ok {
	// 					syslog(fmt.Sprintf("Error: type name %q not found in types.GetTyShortNm \n", nv.Ty))
	// 					panic(fmt.Errorf("Error: type name %q not found in types.GetTyShortNm \n", nv.Ty))
	// 					return
	// 				}

	// 				sk = "S#:" + nv.C + "#" + strconv.Itoa(i)
	// 				mut.SetKey(NodeScalarTbl, UID, sk)
	// 				mut.AddMember(sk, nv.Name)
	// 				mut.AddMember(sk, s)
	// 				mut.AddMember(sk, tyShortNm)
	// 				//a := Item{PKey: UID, SortK: sk, P: nv.Name, S: s, Ty: tyShortNm} //nv.Ty}
	// 				inputs.Add(inputs, input)
	// 			}
	// 		}

	// 	case "SI":

	// 		type Item struct {
	// 			PKey  []byte
	// 			SortK string
	// 			P     string // Dynamo will use AV List type - will convert to SS in convertSet2list()
	// 			N     float64
	// 			Ty    string
	// 		}
	// 		var sk string
	// 		input := db.NewInput(UID)
	// 		if si, ok := nv.Value.([]int); ok {
	// 			//
	// 			for i, s := range si {

	// 				if tyShortNm, ok = types.GetTyShortNm(nv.Ty); !ok {
	// 					syslog(fmt.Sprintf("Error: type name %q not found in types.GetTyShortNm \n", nv.Ty))
	// 					panic(fmt.Errorf("Error: type name %q not found in types.GetTyShortNm \n", nv.Ty))
	// 					return
	// 				}

	// 				sk = "S#:" + nv.C + "#" + strconv.Itoa(i)
	// 				mut.SetKey(NodeScalarTbl, UID, sk)
	// 				mut.AddMember(sk, nv.Name)
	// 				mut.AddMember(sk, float64(s))
	// 				mut.AddMember(sk, tyShortNm)
	// 				//a := Item{PKey: UID, SortK: sk, P: nv.Name, N: float64(s), Ty: tyShortNm} //nv.Ty}
	// 				inputs.Add(inputs, input)
	// 			}
	// 		}

		// case "LS":

		// 	var sk string
		// 	if ss, ok := nv.Value.([]string); ok {
		// 		//
		// 		input := db.NewInput(UID)
		// 		for i, s := range ss {

		// 			sk = "S#:" + nv.C + "#" + strconv.Itoa(i)
		// 			mut.SetKey(NodeScalarTbl, UID, sk)
		// 			mut.AddMember(sk, nv.Name)
		// 			mut.AddMember(sk, s)
		// 			//mut.AddMember(sk,  tyShortNm) //TODO: should this be included?
		// 			inputs.Add(inputs, input)
		// 			//a := Item{PKey: UID, SortK: sk, P: nv.Name, S: s}
		// 		}
		// 	}

		//}
		//
	//}

}
