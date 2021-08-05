package client

import (
	//"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	blk "github.com/GoGraph/block"
	gerr "github.com/GoGraph/dygerror"
	"github.com/GoGraph/tx"

	"github.com/GoGraph/cache"
	"github.com/GoGraph/db"
	"github.com/GoGraph/ds"
	"github.com/GoGraph/event"
	mon "github.com/GoGraph/gql/monitor"
	"github.com/GoGraph/rdf/anmgr"
	"github.com/GoGraph/rdf/errlog"
	"github.com/GoGraph/rdf/grmgr"
	"github.com/GoGraph/types"
	//	"github.com/GoGraph/rdf/uuid"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/util"
)

const (
	logid         = "AttachNode"
	propagatedTbl = "PropagatedScalar"
)

var client spanner.Client

func init() {
	client = dbConn.New()
}

func UpdateValue(cUID util.UID, sortK string) error {
	// for update node predicate (sortk)
	// 1. perform cache update first.
	// 2. synchronous update to dynamo plus add stream CDI
	// 4. streams process: to each parent of cUID propagate value.(Streams api: propagateValue(pUID, sortk, v interface{}).

	// for AttachNode
	// for each child scalar create a CDI triggering api propagateValue(pUID, sortk, v interface{}).
	return nil
}

func GetStringValue(cUID util.UID, sortK string) (string, error) { return "", nil }

func IndexMultiValueAttr(cUID util.UID, sortK string) error { return nil }

// sortK is parent's uid-pred to attach child node too. E.g. G#:S (sibling) or G#:F (friend) or A#G#:F It is the parent's attribute to attach the child node.
// pTy is child type i.e. "Person". This could be derived from child's node cache data.
func AttachNode(cUID, pUID util.UID, sortK string, e_ *anmgr.Edge, wg_ *sync.WaitGroup, lmtr *grmgr.Limiter) { // pTy string) error { // TODO: do I need pTy (parent Ty). They can be derived from node data. Child not must attach to parent attribute of same type
	//
	// update db only (cached copies of node are not updated) to reflect child node attached to parent. This involves
	// 1. append chid UID to the associated parent uid-predicate, parent e.g. sortk A#G#:S
	// 2. propagate child scalar data to associated uid-predicate (parent's 'G' type) G#:S#:A etc..
	//
	defer anmgr.AttachDone(e_)
	defer wg_.Done()
	lmtr.StartR()
	defer lmtr.EndR()

	var (
		eID              util.UID
		pnd              *cache.NodeCache
		cTyName, pTyName string
		ok               bool
		err              error
		wg               sync.WaitGroup
	)
	syslog := func(s string) {
		slog.Log("AttachNode: ", s)
	}
	gc := cache.NewCache()
	//
	// log Event via defer
	//
	defer func() func() {
		t0 := time.Now()
		return func() {
			t1 := time.Now()
			if err != nil {
				event.LogEventFail(eID, t1.Sub(t0).String(), err) // TODO : this should also create a CW log event
			} else {
				event.LogEventSuccess(eID, t1.Sub(t0).String())
			}
		}
	}()()

	syslog(fmt.Sprintf(" About to join cUID --> pUID       %s -->  %s  %s", util.UID(cUID).String(), util.UID(pUID).String(), sortK))

	//
	// this API deals only in UID that are known to exist - hence NodeExists() not necessary
	//
	// if ok, err := db.NodeExists(cUID); !ok {
	// 	if err == nil {
	// 		return addErr(fmt.Errorf("Child node UUID %q does not exist:", cUID))
	// 	} else {
	// 		return addErr(fmt.Errorf("Error in validating child node %w", err))
	// 	}
	// }
	// if ok, err := db.NodeExists(pUID, sortK); !ok {
	// 	if err == nil {
	// 		return addErr(fmt.Errorf("Parent node and/or attachment predicate for UUID %q does not exist", pUID))
	// 	} else {
	// 		return addErr(fmt.Errorf("Error in validating parent node %w", err))
	// 	}
	// }
	// create channels used to pass target UID for propagation and errors
	xch := make(chan blk.chPayload)
	defer close(xch)
	//
	// NOOP condition aka CEG - Concurrent event gatekeeper. Add edge only if it doesn't already exist (in one atomic unit) that can be used to protect against identical concurrent (or otherwise) attachnode events.
	//
	// TODO: fix bugs in edgeExists algorithm - see bug list
	if ok, err := EdgeExists(cUID, pUID, sortK, db.ADD); ok {
		if errors.Is(err, db.ErrConditionalCheckFailed) {
			errlog.Add(logid, err)
		} else {
			errlog.Add(logid, fmt.Errorf("AttachNode  db.EdgeExists errored: %w ", err))
		}
		return
	}
	//
	// log Event
	//
	// going straight to db is safe provided its part of a FetchNode lock and all updates to the "R" predicate are performed within the FetchNode lock.
	ev := event.AttachNode{CID: cUID, PID: pUID, SK: sortK}
	//eID, err = eventNew(ev)
	eID, err = event.New(ev)
	if err != nil {
		return
	}
	//
	cTx := tx.New("Propagate Child Scalars") // transacation label, not operator
	//
	wg.Add(1)
	var childErr error
	//
	go func() {
		defer wg.Done()
		//
		// Select child scalar data (sortk: A#A#, non-scalars start with A#B..A#F) and lock child node. Unlocked in UnmarshalCache and defer.(?? no need for cUID lock after Unmarshal - I think?)  ALL SCALARS SHOUD BEGIN WITH sortk "A#A#"
		// A node may not have any scalar values (its a connecting node in that case), but there should always be a A#A#T item defined which defines the type of the node
		//
		cnd, err := gc.FetchForUpdate(cUID, "A#A#")
		defer cnd.Unlock()
		if err != nil {
			errlog.Add(logid, fmt.Errorf("Error fetching child scalar data: %w", err))
			childErr = err
			return
		}
		//
		// get type of child node from A#T sortk e.g "Person"
		//
		if cTyName, ok = cnd.GetType(); !ok {
			errlog.Add(logid, cache.NoNodeTypeDefinedErr)
			return
		}
		//
		// get type definition from type table for child node
		//
		var cty blk.TyAttrBlock // note: this will load cache.TyAttrC -> map[Ty_Attr]blk.TyAttrD
		if cty, err = types.FetchType(cTyName); err != nil {
			errlog.Add(logid, err)
			return
		}
		//
		//***************  wait for payload from concurrent routine ****************
		//
		var py chPayload
		// prevent panic on closed channel by using bool test on channel.
		if py, ok = <-xch; !ok {
			return
		}
		if py.TUID == nil {
			//panic(fmt.Errorf("errored: target UID is nil for  cuid: %s   pUid: %s", cUID, pUID))
			errlog.Add(logid, fmt.Errorf("Received on channel: target UID of nil, cuid: %s   pUid: %s  sortK: %s", cUID, pUID, sortK))
			return
		}
		//
		// add child UID to Upred item (in parent block or overflow block)
		//
		if py.TUID == pUID {
			// in parent block
			upd := tx.NewMutation(EdgeTbl, pUID, sortK, tx.Append)
			upd.AddMember("Nd", cUID)
			upd.AddMember("XF", blk.ChildUID)
			upd.AddMember("Id", 0)
			cTx.Add(upd)
		} else {
			// in overflow block - special case of tx.Append as it will set XF to OvflItemFull if params.OvfwBatchSize exceeded in Nd/XF size.
			// propagateTarget() will use OvflItemFull to create a new batch next time it is executed.
			r := tx.WithOBatchLimit{Ouid: py.TUID, Cuid: cUID, Puid: pUID, DI: py.DI, OSortK: py.Osortk, Index: py.NdIndex}
			upd := tx.NewMutation(EdgeTbl, py.TUID, Osortk, r)
			cTx.Add(upd)
		}
		//
		// build NVclient based on Type info - either all scalar types or only those  declared in IncP attruibte for the attachment type define in sortk
		//
		var cnv ds.ClientNV
		//
		// find attachment data type from sortK eg. A#G#:S
		// here S is simply the abreviation for the Ty field which defines the child type  e.g 	"Person"
		//
		s := strings.LastIndex(sortK, "#")
		attachPoint := sortK[s+2:]
		var found bool
		for _, v := range py.PTy {
			if v.C == attachPoint {
				found = true
				// grab all scalars from child type if the attribute has propagaton enabled or the attribute is nullable (meaning it may or may not be defined)
				// we need to propagate not nulls to support the has() as its the only to know if its defined for the child as the XF(?) attribute will be true if its defined or false if not.
				for _, v := range cty {
					switch v.DT {
					case "I", "F", "Bl", "S", "DT": // Scalar types. TODO: these attributes should belong to pUpred type only. Can a node be made up of more than one type? Pesuming at this stage only 1, so all scalars are relevant.
						if v.Pg || v.N {
							nv := &ds.NV{Name: v.Name}
							cnv = append(cnv, nv)
						}
					}
				}
			}
		}
		if !found {
			panic(fmt.Errorf("Attachmment predicate %q not round in parent", attachPoint)) //TODO - handle as error
		}

		if len(cnv) > 0 {
			//
			// copy cache data into cnv and unlock child node.
			//
			err = cnd.UnmarshalCache(cnv)
			if err != nil {
				errlog.Add(logid, fmt.Errorf("AttachNode (child node): Unmarshal error : %s", err))
				return
			}

			//
			// GetTargetforUpred() has primed the target propagation block with cUID and XF Inuse flag. Ready for propagation of Scalar data.
			// lock pUID if it is the target of the data propagation.
			// for overflow blocks the entry in the Nd of the uid-pred is set to InUse which syncs access.

			for i, t := range cty {

				for _, v := range cnv {

					if t.Name == v.Name {

						cTx.Add(propagateScalar(t, pUID, sortK, py.TUID, py.BatchId, v.Value))

						break
					}
				}
			}
		}
		// add parent UID to reverse edge on child node
		cTx.Add(updateReverseEdge(cUID, pUID, py.TUID, sortK, py.BatchId))

		cnd.ClearNodeCache()
	}()

	handleErr := func(err error) {
		pnd.Unlock()
		errlog.Add(logid, err)
		// send empty payload so concurrent routine will abort -
		// not necessary to capture nil payload error from routine as it has a buffer size of 1
		xch <- chPayload{}
		wg.Wait()
	}
	uTx := tx.New(tx.TargetUPred)
	//
	//fetch and lock parent node. This prevents concurrent Attach node operations on this node either as child or parent.
	pnd, err = gc.FetchUIDpredForUpdate(pUID, sortK)
	defer pnd.Unlock()
	if err != nil {
		handleErr(fmt.Errorf("main errored in FetchForUpdate: for %s errored..%w", pUID, err))
		return
	}
	//
	// get type of node from A#T sortk e.g "Person"
	//
	if pTyName, ok = pnd.GetType(); !ok {
		handleErr(fmt.Errorf(fmt.Sprintf("AttachNode: Error in GetType of parent node")))
		return
	}
	syslog(fmt.Sprintf("in main, pTyName %s sortk %q  pUID  %s", pTyName, sortK, pUID))
	//
	// get type details from type table for child node
	//
	var pty blk.TyAttrBlock
	if pty, err = types.FetchType(pTyName); err != nil {
		handleErr(fmt.Errorf("AttachNode main: Error in types.FetchType : %w", err))
		return
	}

	cpy := &blk.ChPayLoad{PTy: pty}

	pnd.PropagationTarget(uTx, cpy, sortK, pUID, cUID) // TODO - don't saveConfigUpred until child node successfully joined. Also clear cache entry for uid-pred on parent - so it must be read from storage.

	xch <- cpy

	wg.Wait()

	if childErr != nil {
		err = childErr
		syslog(fmt.Sprintf("AttachNode (cUID->pUID: %s->%s %s) failed Error: %s", cUID, pUID, sortK, childErr))
		pnd.ClearCache(sortK, true)
		panic(fmt.Errorf("AttachNode (cUID->pUID: %s->%s %s) failed Error: %s", cUID, pUID, sortK, childErr))
	}
	// the cache is not maintained during the attach node opeation so clear the cache
	// forcing a physcal read on next fetch node request
	pnd.ClearNodeCache()
	// run all the database requests as a transaction (if possible)
	//
	// process overflow block mutations
	//
	err = uTx.Execute()
	if err != nil {
		panic(err)
	}
	//
	// process scalar propagation and child attach to parent mutations
	//
	err = cTx.Execute()
	if err != nil {
		panic(err)
	}
	// TODO: log cTx, uTx
	//
	// monitor: increment attachnode counter
	//
	stat := mon.Stat{Id: mon.AttachNode}
	mon.StatCh <- stat

}

// DetachNode: Not implemented for Spanner....
func DetachNode(cUID, pUID util.UID, sortK string) error {
	//

	var (
		err error
		ok  bool
		eID util.UID
	)

	ev := event.DetachNode{CID: cUID, PID: pUID, SK: sortK}
	eID, err = event.New(ev)
	if err != nil {
		return fmt.Errorf("Error in DetachNode creating an event: %s", err)
	}
	// log Event via defer
	defer func() func() {
		t0 := time.Now()
		return func() {
			t1 := time.Now()
			if err != nil {
				event.LogEventFail(eID, t1.Sub(t0).String(), err) // TODO : this should also create a CW log event. NO THIS IS PERFORMED BY STREAMS Lambda function.
			} else {
				event.LogEventSuccess(eID, t1.Sub(t0).String())
			}
		}
	}()()
	//
	// CEG - Concurrent event gatekeeper.
	//
	if ok, err = EdgeExists(cUID, pUID, sortK, db.DELETE); !ok {
		if errors.Is(err, db.ErrConditionalCheckFailed) {
			return gerr.NodesNotAttached
		}
	}
	if err != nil {
		return err
	}
	//err = db.DetachNode(cUID, pUID, sortK)
	if err != nil {
		var nif db.DBNoItemFound
		if errors.As(err, &nif) {
			err = nil
			fmt.Println(" returning with error NodesNotAttached..............")
			return gerr.NodesNotAttached
		}
		return err
	}

	return nil
}

func updateReverseEdge(cuid, puid, tUID util.UID, sortk string, batchId int) *tx.Mutation {
	//
	// BS : set of binary values representing puid + tUID + sortk(last 2 entries). Used to determine the tUID the child data saved to.
	// not used anymore: PBS : set of binary values representing puid + sortk (last entry). Can be used to quickly access if child is attached to parent
	pred := func(sk string) string {
		s_ := strings.SplitAfterN(sk, "#G#", -1) // A#G#:S#:D#3
		if len(s_) == 0 {
			panic(fmt.Errorf("buildExprValues: SortK of %q, must have at least one # separator", sk))
		}
		return s_[1] //  return :S#:D#3
	}
	// get size of BS array
	// has its own overflow block - ReverseEdgeTarget()
	// XF - does not exist unilt BS size reaches params.OvfwBatchSize which is then populated with Ouid generated in ReverseEdgeTarget().

	sortk += "#" + strconv.Itoa(batchId)
	bs := make([][]byte, 1, 1) // representing a binary set.
	bs[0] = append(puid, []byte(tUID)...)
	bs[0] = append(bs[0], pred(sortk)...)
	//
	mut := db.NewMutation(edgeTbl, cuid, "R#", tx.Append)
	mut.AddMember("BS", bs)

	return mut

}

// EdgeExists acts as a sentinel or CEG - Concurrent event gatekeeper, to the AttachNode and DetachNode operations.
// It guarantees the event (operation + data) can only run once.
// Rather than check parent is attached to child, ie. for cUID in pUID uid-pred which may contain millions of UIDs spread over multiple overflow blocks more efficient
// to check child is attached to parent in cUID's #R attribute.
// Solution: specify field "BS" and  query condition 'contains(PBS,pUID+"f")'          where f is the short name for the uid-pred predicate - combination of two will be unique
//           if update errors then node is not attached to that parent-node-predicate, so nothing to delete
//
func EdgeExists(cuid, puid util.UID, sortk string, action byte) (bool, error) {

	inputs := db.NewInputs()
	input := db.NewInput()

	if param.DebugOn {
		fmt.Println("In EdgeExists: on ", cuid, puid, sortk)
	}
	//
	fmt.Println("In EdgeExists: on ", cuid, puid, sortk)

	input.SetKey(propagatedTbl, cuid, "R#")

	pred := func(sk string) string {
		i := strings.LastIndex(sk, "#")
		return sk[i+2:]
	}
	// note EdgeExists is only called on known nodes - so not necessary to check nodes exist.
	//
	// if ok, err := NodeExists(cuid); !ok {
	// 	if err != nil {
	// 		return false, fmt.Errorf("Child node %s does not exist:", cuid)
	// 	} else {
	// 		return false, fmt.Errorf("Error in NodeExists %w", err)
	// 	}
	// }
	// if ok, err := NodeExists(puid, sortk); !ok {
	// 	if err != nil {
	// 		return false, fmt.Errorf("Parent node and/or attachment predicate %s does not exist")
	// 	} else {
	// 		return false, fmt.Errorf("Error in NodeExists %w", err)
	// 	}
	// }
	//
	// if the operation is AttachNode we want to ADD the parent node onlyif parent node does not exist otherwise error
	// if the operation is DetachNode we want to DELETE parent node only if parent node exists otherwise error
	//
	//  a mixture of expression and explicit AttributeValue definitions is used - to overcome idiosyncrasies in Dynmaodb sdk handling of Sets
	switch action {

	case DELETE:
		opr = db.NewOperation(db.EdgeExistsDetachNode)

		pbs := make([][]byte, 1, 1)
		pbs[0] = append(puid, pred(sortk)...)
		var pbsC []byte
		pbsC = append(puid, pred(sortk)...)
		// bs is removed in: removeReverseEdge which requires target UID which is not availabe when EdgeExists is called
		input.AddMember("PBS", "@v1", pbs)
		//upd = expression.Delete(expression.Name("PBS"), expression.Value(pbs))
		input.AddConditiion("Contains", "PBS", pbsC, "Not")
		// Contains requires a string for second argument however we want to put a B value. Use X as dummyy to be replaced in explicit AttributeValue stmt
		//cond = expression.Contains(expression.Name("PBS"), "X")
		// replace gernerated AttributeValue values with corrected ones.
		eav = map[string]*dynamodb.AttributeValue{":0": &dynamodb.AttributeValue{B: pbsC}, ":1": &dynamodb.AttributeValue{BS: pbs}}

	case ADD:
		opr = db.NewOperation(db.EdgeExistsDetachNode)

		pbs := make([][]byte, 1, 1)
		pbs[0] = append(puid, pred(sortk)...)
		var pbsC []byte
		pbsC = append(puid, pred(sortk)...)
		//
		input.AddMember("PBS", pbs)
		//upd = expression.Add(expression.Name("PBS"), expression.Value(pbs))
		// Contains - sdk requires a string for second argument however we want to put a B value. Use X as dummyy to be replaced by explicit AttributeValue stmt
		input.AddConditiion("Contains", "PBS", pbsC, "Not")
		//cond = expression.Contains(expression.Name("PBS"), "X").Not()
		// workaround: as expression will want Contains(predicate,<string>), but we are comparing Binary will change in ExpressionAttributeValue to use binary attribute value.
		// also: compare with binary value "v" which is not base64 encoded as the sdk will encode during transport and dynamodb will decode and compare UID (16byte) with UID in set.
		//eav = map[string]*dynamodb.AttributeValue{":0": &dynamodb.AttributeValue{B: pbsC}, ":1": &dynamodb.AttributeValue{BS: pbs}}
	}
	inputs.Add(input)

	inputs.EdgeTest(db.Update) //TODO: implement - see EdgeExists in dynamodb.go
}

// propagateScalar appends each child node scalar data to the parent item associated with the scalar attribute (ie. by its own sortk)
// Each Scalar attribute(column) is represented by its own item/row in the table with its own sortk value e.g. "A#G#:C", and
// array/list attribute type to which each instance of the scalar value is appended.
// The data is merged. If the item does not exist in the parent node block it is inserted. All subsequent scalar values are updated by
// appending to the attribute type (List/Array)
func propagateScalar(ty blk.TyAttrD, pUID util.UID, sortK string, tUID util.UID, batchId int, value interface{}) *tx.Mutation { //, wg ...*sync.WaitGroup) error {
	// **** where does Nd, XF get updated when in Overflow mode.???
	//
	var (
		lty   string
		sortk string
		err   error
	)
	//lveu-vwfs-xfyd-wgmi

	if bytes.Equal(pUID, tUID) {
		if ty.DT != "Nd" {
			// simple scalar e.g. Age
			lty = "L" + ty.DT
			sortk = sortK + "#:" + ty.C // TODO: currently ignoring concept of partitioning data within node block. Is that right?
		} else {
			// TODO: can remove this section
			// uid-predicate e.g. Sibling
			lty = "Nd"
			//	sortk = "A#G#:" + sortK[len(sortK)-1:] // TODO: currently ignoring concept of partitioning data within node block. Is that right? Fix: this presumes single character short name
			sortk = "A#G#:" + sortK[strings.LastIndex(sortK, ":")+1:]
		}
	} else {
		// append data to overflow block batch
		if ty.DT != "Nd" {
			// simple scalar e.g. Age
			lty = "L" + ty.DT
			sortk = sortK + "#:" + ty.C + "%" + strconv.Itoa(batchId) // TODO: currently ignoring concept of partitioning data within node block. Is that right?
		}
		// else {
		// 	// TODO: can remove this section
		// 	// uid-predicate e.g. Sibling
		// 	lty = "Nd"
		// 	//sortk = "A#G#:" + sortK[len(sortK)-1:] // TODO: currently ignoring concept of partitioning data within node block. Is that right? Fix: this presumes single character short name
		// 	sortk = "A#G#:" + sortK[strings.LastIndex(sortK, ":")+1:]
		// }
	}
	//
	// dml - append to parent block uid-pred (sortk) or overflow block batch
	//
	txm := tx.NewMutation(propagatedTbl, tUID, sortk, tx.PropagateMerge)
	//
	// shadow XBl null identiier. Here null means there is no predicate specified in item, so its value is necessarily null (ie. not defined)
	//
	null := make([]bool, 1, 1)
	// no predicate value in item - set associated null flag, XBl, to true
	if value == nil {
		null[0] = true
	}
	// append child attr value to parent uid-pred list

	switch lty {

	case "LI", "LF":
		// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
		if value == nil {
			//null[0] = true // performed above
			switch ty.DT {
			case "I":
				value = int64(0)
			case "F":
				value = float64(0)
			}
		}

		switch x := value.(type) {
		case int:
			v := make([]int, 1, 1)
			v[0] = x
			txm.AddMember("LI", v)
		case int32:
			v := make([]int32, 1, 1)
			v[0] = x
			txm.AddMember("LI", v)
		case int64:
			v := make([]int64, 1, 1)
			v[0] = x
			txm.AddMember("LI", v)
		case float64:
			v := make([]float64, 1, 1)
			v[0] = x
			txm.AddMember("LF", v)
		default:
			// TODO: check if string - ok
			panic(fmt.Errorf("data type must be a number, int64, float64"))
		}

	case "LBl":
		if value == nil {
			value = false
		}
		if x, ok := value.(bool); !ok {
			logerr(fmt.Errorf("data type must be a bool"), true)
		} else {
			v := make([]bool, 1, 1)
			v[0] = x
			txm.AddMember(lty, v)
		}

	case "LS":
		if value == nil {
			value = "__NULL__"
		}
		if x, ok := value.(string); !ok {
			logerr(fmt.Errorf("data type must be a string"), true)
		} else {
			v := make([]string, 1, 1)
			v[0] = x
			txm.AddMember(lty, v)
		}

	case "LDT":

		if value == nil {
			value = "__NULL__"
		}
		if x, ok := value.(time.Time); !ok {
			logerr(fmt.Errorf("data type must be a time"), true)
		} else {
			v := make([]string, 1, 1)
			v[0] = x.String()
			txm.AddMember(lty, v)
		}

	case "LB":

		if value == nil {
			value = []byte("__NULL__")
		}
		if x, ok := value.([]byte); !ok {
			logerr(fmt.Errorf("data type must be a byte slice"), true)
		} else {
			v := make([][]byte, 1, 1)
			v[0] = x
			txm.AddMember(lty, v)
		}

	}
	//
	// bool represents if entry passed in is defined or not. True means it is not defined equiv to null entry.
	//
	txm.AddMember("XBl", null)
	//
	// Marshal primary key of parent node
	//
	return txm
}
