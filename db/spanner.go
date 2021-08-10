package db

import (
	"context"
	"fmt"
	"strings"

	blk "github.com/GoGraph/block"
	"github.com/GoGraph/dbConn"
	//gerr "github.com/GoGraph/dygerror"
	//mon "github.com/GoGraph/gql/monitor"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/util"

	"cloud.google.com/go/spanner" //v1.21.0
)

const (
	DELETE = 'D'
	ADD    = 'A'
	// spanner table names
	BlockTbl      = "Block"
	EdgeTbl       = "Edge"
	NodeScalarTbl = "NodeScalar"
	PropagatedTbl = "PropagatedScalar"
)

type Block struct {
	Pkey []byte
	Ty   string   // Node type
	P    util.UID // Overflow block (UID ptr to Parent Node)
}
type NodeScalar struct {
	Pkey  util.UID
	Sortk string
	//
	Ty string
	//
	Bl bool
	S  string
	F  float64 // what about integer??
	I  int64
	B  []byte
	DT string
	//
	LS  []string
	LI  []int64
	LF  []float64
	LBl []bool
	LB  [][]byte
	LDT []string
	//
	SS []string
	NS []int64
	BS [][]byte
}
type Edges struct {
	Pkey  util.UID
	Sortk string
	//
	Nd [][]byte
	Id []int
	XF []int
	// P, Ty, N (see SaveUpredState) ???
	//
	PBS [][]byte
	BS  [][]byte
}

type PropagatedScalar struct {
	Pkey  util.UID
	Sortk string
	//
	LS  []string
	LI  []int64
	LF  []float64
	LBl []bool
	LDT []string
	LB  [][]byte
	// determines if slice entry is null (true), default false
	XBl []bool
}

type Bdi struct {
	N  Block               `spanner:"n"`
	Ns []*NodeScalar       `spanner:"ns"`
	E  []*Edges            `spanner:"e"`
	Ps []*PropagatedScalar `spanner:"ps"`
}

type gsiResult struct {
	Pkey  []byte
	SortK string
}

var (
	client *spanner.Client
)

func init() {
	client = dbConn.New()
}

//  ItemCache struct is the transition between Dynamodb types and the actual attribute type defined in the DD.
//  Number (dynamodb type) -> float64 (transition) -> int (internal app & as defined in DD)
//  process: dynamodb -> ItemCache -> DD conversion if necessary to application variables -> ItemCache -> Dynamodb
//	types that require conversion from ItemCache to internal are:
//   DD:   int         conversion: float64 -> int
//   DD:   datetime    conversion: string -> time.Time
//  all the other datatypes do not need to be converted.

func logerr(e error, panic_ ...bool) {

	if len(panic_) > 0 && panic_[0] {
		slog.Log("DB: ", e.Error(), true)
		panic(e)
	}
	slog.Log("DB: ", e.Error())
}

func syslog(s string) {
	slog.Log("DB: ", s)
}

//  NOTE: tyShortNm is duplicated in cache pkg. It exists in in db package only to support come code in rdfload.go that references the db version rather than the cache which it cannot access
// because of input-cycle issues. Once this reference is rdfload is removed the cache version should be the only one used.

type tyNames struct {
	ShortNm string `json:"Atr"`
	LongNm  string
}

var (
	err       error
	tynames   []tyNames
	tyShortNm map[string]string
)

func GetTypeShortNames() ([]tyNames, error) {
	return tynames, nil
}

// FetchNode performs a Query with KeyBeginsWidth on the SortK value, so all item belonging to the SortK are fetched.
func FetchNode(uid util.UID, subKey ...string) (blk.NodeBlock, error) {

	var (
		sortk string
	)

	ctx := context.Background()

	if len(subKey) > 0 {
		sortk = subKey[0]
		slog.Log("DB FetchNode: ", fmt.Sprintf(" node: %s subKey: %s", uid.String(), sortk))
	} else {
		sortk = "A#"
		slog.Log("DB FetchNode: ", fmt.Sprintf(" node: %s subKey: %s", uid.String(), sortk))
	}
	//defer client.Close()

	params := map[string]interface{}{"uid": uid, "sk": sortk}
	// stmt returns one row
	sql := `Select *,
					ARRAY (select as struct * from NodeScalar       ns where ns.UID = n.UID and ns.Sortk = Starts_With(@sk)) as ns,
					ARRAY (select as struct * from Edge             e  where  e.UID = n.UID and  e.Sortk = Starts_With(@sk)) as e,
					ARRAY (select as struct * from PropagatedScalar ps where ps.UID = n.UID and ps.Sortk = Starts_With(@sk)) as ps
			   from Block n
			   where n.Pkey = @uid`

	iter := client.Single().Query(ctx, spanner.Statement{SQL: sql, Params: params})
	//
	// load into a NodeBlock
	//
	var nb blk.NodeBlock

	err = iter.Do(func(r *spanner.Row) error {
		// for each row - however only one row is return from db.
		//
		// Unmarshal database output into Bdi
		//
		rec := &Bdi{}
		r.ToStruct(rec)

		nbrow := &blk.DataItem{}
		nbrow.Pkey = rec.N.Pkey
		nbrow.Ty = rec.N.Ty
		nb = append(nb, nbrow)

		for _, k := range rec.Ns {
			nbrow := &blk.DataItem{}
			nbrow.Pkey = k.Pkey
			nbrow.Sortk = k.Sortk
			nbrow.Ty = k.Ty
			nbrow.Bl = k.Bl
			nbrow.S = k.S
			nbrow.I = k.I
			nbrow.F = k.F
			nbrow.B = k.B
			nbrow.DT = k.DT
			//
			nbrow.LS = k.LS
			nbrow.LI = k.LI
			nbrow.LF = k.LF
			nbrow.LB = k.LB
			nbrow.LBl = k.LBl
			//
			// nbrow.NS  = k.NS
			// nbrow.SS  = k.SS
			// nbrow.BS  = k.BS

			nb = append(nb, nbrow)
		}

		for _, k := range rec.E {
			nbrow := &blk.DataItem{}
			nbrow.Pkey = k.Pkey
			nbrow.Sortk = k.Sortk
			if k.Sortk == "R#" {
				nbrow.PBS = k.PBS
				nbrow.BS = k.BS
			} else {
				nbrow.Nd = k.Nd
				nbrow.XF = k.XF
				nbrow.Id = k.Id
			}

			nb = append(nb, nbrow)
		}

		//case len(rec.Ps) != 0:

		nbrow.Pkey = rec.N.Pkey
		for _, k := range rec.Ps {
			nbrow := &blk.DataItem{}
			nbrow.Pkey = k.Pkey
			nbrow.Sortk = k.Sortk
			nbrow.LS = k.LS
			nbrow.LI = k.LI
			nbrow.LF = k.LF
			nbrow.LBl = k.LBl
			nbrow.LDT = k.LDT
			nbrow.LB = k.LB
			nbrow.XBl = k.XBl

			nb = append(nb, nbrow)
		}

		return nil
	})

	if len(nb) == 0 {
		// is subKey a G type (uid-predicate) ie. child data block associated with current parent node, create empty dataItem.
		if len(subKey) > 0 && strings.Index(subKey[0], "#G#") != -1 {
			data := make(blk.NodeBlock, 1)
			data[0] = new(blk.DataItem)
			return data, nil
		}
		return nil, newDBNoItemFound("FetchNode", uid.String(), "", "Query")
	}
	//
	// send stats
	//
	// v := mon.Fetch{CapacityUnits: *result.ConsumedCapacity.CapacityUnits, Items: len(result.Items), Duration: dur}
	// stat := mon.Stat{Id: mon.DBFetch, Value: &v}
	// mon.StatCh <- stat

	return nb, nil

}

func FetchNodeItem(uid util.UID, sortk string) (blk.NodeBlock, error) {

	// stat := mon.Stat{Id: mon.DBFetch}
	// mon.StatCh <- stat

	// proj := expression.NamesList(expression.Name("SortK"), expression.Name("Nd"), expression.Name("XF"), expression.Name("Id"))
	// expr, err := expression.NewBuilder().WithProjection(proj).Build()
	// if err != nil {
	// 	return nil, newDBExprErr("FetchNodeItem", "", "", err)
	// }
	// TODO: remove encoding when load of data via cli is not used.

	params := map[string]interface{}{"uid": uid, "sk": sortk}

	stmt := `Select *,
					ARRAY (select as struct * from NodeScalar       ns where ns.UID = n.UID and ns.Sortk = @sk) as ns,
					ARRAY (select as struct * from Edge             e  where  e.UID = n.UID and  e.Sortk = @sk) as e,
					ARRAY (select as struct * from PropagatedScalar ps where ps.UID = n.UID and ps.Sortk = @sk) as ps
			from Block n
			where n.Pkey = @uid`

	ctx := context.Background()
	iter := client.Single().Query(ctx, spanner.Statement{SQL: stmt, Params: params})

	//nb:=make(blk.NodeBlock,iter.RowCount,iter.RowCount)
	var nb blk.NodeBlock

	err = iter.Do(func(r *spanner.Row) error {
		nbrow := &blk.DataItem{}

		rec := &Bdi{}
		r.ToStruct(rec)

		nbrow.Pkey = rec.N.Pkey
		nbrow.Ty = rec.N.Ty

		switch {

		case len(rec.Ns) != 0:

			for _, k := range rec.Ns {
				nbrow := &blk.DataItem{}
				nbrow.Sortk = k.Sortk
				nbrow.Ty = k.Ty
				nbrow.Bl = k.Bl
				nbrow.S = k.S
				nbrow.I = k.I
				nbrow.F = k.F
				nbrow.B = k.B
				nbrow.DT = k.DT
				//
				nbrow.LS = k.LS
				nbrow.LI = k.LI
				nbrow.LF = k.LF
				nbrow.LB = k.LB
				nbrow.LBl = k.LBl

				nb = append(nb, nbrow)
			}

		case len(rec.E) != 0:

			nbrow.Pkey = rec.N.Pkey
			for _, k := range rec.E {
				nbrow := &blk.DataItem{}
				nbrow.Sortk = k.Sortk
				if k.Sortk == "R#" {
					nbrow.PBS = k.PBS
					nbrow.BS = k.BS
				} else {
					nbrow.Nd = k.Nd
					nbrow.XF = k.XF
					nbrow.Id = k.Id
				}

				nb = append(nb, nbrow)
			}

		case len(rec.Ps) != 0:

			nbrow.Pkey = rec.N.Pkey
			for _, k := range rec.Ps {
				nbrow := &blk.DataItem{}
				nbrow.Sortk = k.Sortk
				nbrow.LS = k.LS
				nbrow.LI = k.LI
				nbrow.LF = k.LF
				nbrow.LBl = k.LBl
				nbrow.LDT = k.LDT
				nbrow.LB = k.LB
				nbrow.XBl = k.XBl
			}
		}

		nb = append(nb, nbrow)

		return nil
	})

	return nb, nil
	//
}

type Pkey struct {
	Pkey  []byte
	SortK string
}

// SaveCompleteUpred saves all Nd & Xf & Id values. See SaveUpredAvailability which saves an individual UID state.
// func SaveCompleteUpred(cTx *tx.Handle, di *blk.DataItem) error {
// 	//
// 	var err error
// 	upd := NewMuatation(EdgeTbl, di.Pkey, di.SortK, tx.Update)
// 	// update all elements in XF, Id, Nd
// 	upd.AddMember("XF", di.XF)
// 	upd.AddMember("Id", di.Id)
// 	upd.AddMember("Nd", di.Nd)
// 	//
// 	cTx.Add(upd)
// 	// if its a new di, as the propagated sortk does not yet exist, then must insert, otherwise update.
// 	// note: dynamodb will automatically do a merge for an UpdateItem ie. insert if key not present, otherwise update
// 	// spanner sql on the other hand must do an update first, error if not present, then insert.
// 	return nil
// }

// SaveUpredState - transferred to cache package using inputs now...
// SaveUpredAvailability writes availability state of the uid-pred to storage
//func SaveUpredState(di *blk.DataItem, uid util.UID, status int, idx int, cnt int, attrNm string, ty string) error {

// modify the target element in the XF List type.
//syslog(fmt.Sprintf("SaveUpredAvailable: %d %d %d ", status, idx, cnt))
// entry := "XF[" + strconv.Itoa(idx) + "]"
// upd = expression.Set(expression.Name(entry), expression.Value(status))
// upd = upd.Set(expression.Name("P"), expression.Value(attrNm))
// upd = upd.Set(expression.Name("Ty"), expression.Value(ty))
// upd = upd.Add(expression.Name("N"), expression.Value(cnt))
// expr, err = expression.NewBuilder().WithUpdate(upd).Build()
// if err != nil {
// 	return newDBExprErr("SaveUpredAvailable", "", "", err)
// }
// values = expr.Values()
// // convert expression values result from binary Set to binary List
// convertSet2List()
//
// Marshal primary key of parent node
//
// 	Pkey := Pkey{Pkey: di.Pkey, SortK: di.SortK}
// 	av, err := dynamodbattribute.MarshalMap(&Pkey)
// 	if err != nil {
// 		return newDBMarshalingErr("SaveUpredAvailable", util.UID(di.GetPkey()).String(), "", "MarshalMap", err)
// 	}
// 	//
// 	update := &dynamodb.UpdateItemInput{
// 		Key:                       av,
// 		ExpressionAttributeNames:  expr.Names(),
// 		ExpressionAttributeValues: values,
// 		UpdateExpression:          expr.Update(),
// 	}
// 	update = update.SetTableName(param.GraphTable).SetReturnConsumedCapacity("TOTAL")
// 	//
// 	{
// 		t0 := time.Now()
// 		uio, err := dynSrv.UpdateItem(update)
// 		t1 := time.Now()
// 		if err != nil {
// 			return newDBSysErr("SaveUpredAvailable", "UpdateItem", err)
// 		}
// 		syslog(fmt.Sprintf("SaveUpredAvailable:consumed capacity for Query  %s.  Duration: %s", uio.ConsumedCapacity, t1.Sub(t0)))

// 	}
// 	return nil
// }

// SaveOvflBlkFull - overflow block has become full due to child data propagation.
// Mark it as full so it will not be chosen in future to load child data.
// this was called from the cache service.
// func SaveOvflBlkFull(di *blk.DataItem, idx int) error {
// 	return nil
// }

// 	Pkey := Pkey{Pkey: di.Pkey, SortK: di.SortK}

// 	av, err := dynamodbattribute.MarshalMap(&Pkey)
// 	if err != nil {
// 		return newDBMarshalingErr("SaveOvflBlkFull", util.UID(di.Pkey).String(), "", "MarshalMap", err)
// 	}
// 	//
// 	// use cIdx to update XF entry
// 	//
// 	idxs := "XF[" + strconv.Itoa(idx) + "]"
// 	upd := expression.Set(expression.Name(idxs), expression.Value(blk.OvflItemFull))
// 	expr, err := expression.NewBuilder().WithUpdate(upd).Build()
// 	if err != nil {
// 		return newDBExprErr("SaveOvflBlkFull", "", "", err)
// 	}
// 	//
// 	updii := &dynamodb.UpdateItemInput{
// 		Key:                       av,
// 		ExpressionAttributeNames:  expr.Names(),
// 		ExpressionAttributeValues: expr.Values(),
// 		UpdateExpression:          expr.Update(),
// 	}
// 	updii = updii.SetTableName(param.GraphTable).SetReturnConsumedCapacity("TOTAL")
// 	//
// 	{
// 		t0 := time.Now()
// 		uio, err := dynSrv.UpdateItem(updii)
// 		t1 := time.Now()
// 		if err != nil {
// 			return newDBSysErr("SaveOvflBlkFull", "UpdateItem", err)
// 		}
// 		syslog(fmt.Sprintf("SaveOvflBlkFull: consumed capacity for UpdateItem : %s  Duration: %s\n", uio.ConsumedCapacity, t1.Sub(t0)))
// 	}

// 	return nil
// }

// SetCUIDpgFlag is used as part of the recovery when child data propagation when attaching a node exceeds the db item size. This will only happen in the overflow blocks
// which share the item with thousands of child UID.
//
// func SetCUIDpgFlag(tUID, cUID util.UID, sortk string) error {
// 	return nil
// }

// SaveChildUIDtoOvflBlock appends cUID and XF (of ChildUID only) to overflow block
// This data is not cached.
// this function is similar to the mechanics of PropagateChildData (which deals with Scalar data)
// but is concerned with adding Child UID to the Nd and XF attributes.
// func SaveChildUIDtoOvflBlock(cUID, tUID util.UID, sortk string, id int) error { //
// 	return nil
// }

// var (
// 	err    error
// 	expr   expression.Expression
// 	upd    expression.UpdateBuilder
// 	values map[string]*dynamodb.AttributeValue
// )

// //Marshal primary key of parent node

// if param.DebugOn {
// 	fmt.Println("***** SaveChildUIDtoOvflBlock:  ", cUID.String(), tUID.String(), sortk)
// }
// Pkey := Pkey{Pkey: tUID, SortK: sortk}
// av, err := dynamodbattribute.MarshalMap(&Pkey)
// if err != nil {
// 	return newDBMarshalingErr("SaveChildUIDtoOvflBlock", "", "", "MarshalMap", err)
// }
// // add child-uid to overflow block item. Consider using SIZE on attribute ND to limit number of RCUs that need to be consumed.
// // if size was restricted to 100K or 25 RCS that woould be quicker and less costly than 400K item size.
// //
// v := make([][]byte, 1, 1)
// v[0] = []byte(cUID)
// upd = expression.Set(expression.Name("Nd"), expression.ListAppend(expression.Name("Nd"), expression.Value(v)))
// cond := expression.Name("XF").Size().LessThanEqual(expression.Value(param.OvfwBatchLimit))
// //
// // add associated flag values
// //
// xf := make([]int, 1, 1)
// xf[0] = blk.ChildUID
// upd = upd.Set(expression.Name("XF"), expression.ListAppend(expression.Name("XF"), expression.Value(xf)))
// // increment count of nodes
// upd = upd.Add(expression.Name("Cnt"), expression.Value(1))
// //
// expr, err = expression.NewBuilder().WithCondition(cond).WithUpdate(upd).Build()
// if err != nil {
// 	return newDBExprErr("SaveChildUIDtoOvflBlock", "", "", err)
// }
// 	// convert selected attribute from  Set to  List
// 	values = expr.Values()
// 	// expression.Build() will marshal array atributes as SETs rather than Lists.
// 	// Need to convert from BS to LIST as UpdateItem will get errr due to conflict of Set type in dynamodb.AttributeValue and List in database.
// 	convertSet2List()
// 	//
// 	input := &dynamodb.UpdateItemInput{
// 		Key:                       av,
// 		ExpressionAttributeNames:  expr.Names(),
// 		ExpressionAttributeValues: expr.Values(),
// 		UpdateExpression:          expr.Update(),
// 		ConditionExpression:       expr.Condition(),
// 	}
// 	input = input.SetTableName(param.GraphTable).SetReturnConsumedCapacity("TOTAL")
// 	{
// 		t0 := time.Now()
// 		uio, err := dynSrv.UpdateItem(input)
// 		t1 := time.Now()
// 		if err != nil {
// 			return newDBSysErr("SaveChildUIDtoOvflBlock", "UpdateItem", err)
// 		}
// 		syslog(fmt.Sprintf("SaveChildUIDtoOvflBlock: consumed updateitem capacity: %s, Duration: %s\n", uio.ConsumedCapacity, t1.Sub(t0)))
// 	}
// 	return nil

// }

// TODO write the following func called from client issuing AttachNode

//func (pn *NodeCache) GetTargetBlock(sortK string, cUID util.UID) util.UID {
// AddOverflowUIDs(pn, newOfUID) - called from cache.GetTargetBlock
// sortk points to uid-pred e.g. A#G#:S,  which is the target of the data propagation
// func AddOvflUIDs(di *blk.DataItem, OfUIDs []util.UID) error {
// 	return nil
// }

// 	var (
// 		err    error
// 		expr   expression.Expression
// 		upd    expression.UpdateBuilder
// 		values map[string]*dynamodb.AttributeValue
// 	)
// 	//
// 	// Marshal primary key of parent node
// 	//
// 	//	Pkey := Pkey{Pkey: di.GetPkey(), SortK: di.GetSortK()}
// 	Pkey := Pkey{Pkey: di.Pkey, SortK: di.SortK}
// 	av, err := dynamodbattribute.MarshalMap(&Pkey)
// 	//
// 	if err != nil {
// 		return newDBMarshalingErr("AddOvflUIDs", util.UID(di.GetPkey()).String(), "", "MarshalMap", err)
// 	}
// 	// add overflow block uids
// 	//
// 	v := make([][]byte, len(OfUIDs), len(OfUIDs))
// 	for i := 0; i < len(OfUIDs); i++ {
// 		v[i] = []byte(OfUIDs[i])
// 	}
// 	upd = expression.Set(expression.Name("Nd"), expression.ListAppend(expression.Name("Nd"), expression.Value(v)))
// 	//
// 	// add associated flag values
// 	//
// 	xf := make([]int, len(OfUIDs), len(OfUIDs))
// 	for i := 0; i < len(OfUIDs); i++ {
// 		xf[i] = di.XF[i]
// 	}
// 	upd = upd.Set(expression.Name("XF"), expression.ListAppend(expression.Name("XF"), expression.Value(xf)))
// 	//
// 	// add associated item id
// 	//
// 	id := make([]int, len(OfUIDs), len(OfUIDs))
// 	for i := 0; i < len(OfUIDs); i++ {
// 		id[i] = di.Id[i]
// 	}
// 	upd = upd.Set(expression.Name("Id"), expression.ListAppend(expression.Name("Id"), expression.Value(id)))
// 	// increment count of nodes
// 	upd = upd.Add(expression.Name("Cnt"), expression.Value(len(xf)))
// 	//
// 	expr, err = expression.NewBuilder().WithUpdate(upd).Build()
// 	if err != nil {
// 		return newDBExprErr("PropagateChildData", "", "", err)
// 	}
// 	// convert selected attribute from  Set to  List
// 	values = expr.Values()
// 	convertSet2List()
// 	//
// 	input := &dynamodb.UpdateItemInput{
// 		Key:                       av,
// 		ExpressionAttributeNames:  expr.Names(),
// 		ExpressionAttributeValues: expr.Values(),
// 		UpdateExpression:          expr.Update(),
// 	}
// 	input = input.SetTableName(param.GraphTable).SetReturnConsumedCapacity("TOTAL")
// 	{
// 		t0 := time.Now()
// 		uio, err := dynSrv.UpdateItem(input)
// 		t1 := time.Now()
// 		if err != nil {
// 			return newDBSysErr("AddOvflUIDs", "UpdateItem", err)
// 		}
// 		syslog(fmt.Sprintf("AddOvflUIDs: consumed updateitem capacity: %s, Duration: %s\n", uio.ConsumedCapacity, t1.Sub(t0)))
// 	}
// 	return nil

// }

// newUIDTarget - creates a dymamo item to receive cUIDs/XF/Id data. Actual progpagated data resides in items with sortK of
// <target-sortK>#<Id>#:<scalarPred>
// func newUIDTarget(tUID util.UID, sortk string, id int) (map[string]*dynamodb.AttributeValue, error) { // create dummy item with flag value of DELETED. Why? To establish Nd & XF attributes as Lists rather than Sets..
// 	return nil, nil
// }

// 	type TargetItem struct {
// 		Pkey  []byte
// 		SortK string
// 		Nd    [][]byte
// 		XF    []int
// 	}

// 	nilItem := []byte("0")
// 	nilUID := make([][]byte, 1, 1)
// 	nilUID[0] = nilItem
// 	//
// 	xf := make([]int, 1, 1)
// 	xf[0] = blk.UIDdetached // this is a nil (dummy) entry so mark it deleted. Used to append other cUIDs too.
// 	//
// 	// create a "batch" sortk
// 	//
// 	var s strings.Builder
// 	s.WriteString(sortk)
// 	s.WriteByte('#')
// 	s.WriteString(strconv.Itoa(id)) //  <target-sortK>#<Id>
// 	//
// 	a := TargetItem{Pkey: tUID, SortK: s.String(), Nd: nilUID, XF: xf}
// 	av, err := dynamodbattribute.MarshalMap(a)
// 	if err != nil {
// 		return nil, fmt.Errorf("newUIDTarget: ", err.Error())
// 	}
// 	return av, nil
// }

// db.MakeOverflowBlock(ofblk)
//func MakeOvflBlocks(ofblk []*blk.OverflowItem, di *blk.DataItem) error {
// func CreateOvflBatch(tUID util.UID, sortk string, id int) error {
// 	return nil
// }

// 	av, err := newUIDTarget(tUID, sortk, id)
// 	if err != nil {
// 		return err
// 	}
// 	convertSet2list(av)

// 	t0 := time.Now()
// 	ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
// 		TableName:              aws.String(param.GraphTable),
// 		Item:                   av,
// 		ReturnConsumedCapacity: aws.String("TOTAL"),
// 	})
// 	t1 := time.Now()
// 	syslog(fmt.Sprintf("CreateOvflBatch: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
// 	if err != nil {
// 		return newDBSysErr("CreateOvflBatch", "PutItem", err)
// 	}

// 	return nil
// }

// db.MakeOverflowBlock(ofblk)
// //func MakeOvflBlocks(ofblk []*blk.OverflowItem, di *blk.DataItem) error {
// func MakeOvflBlocks(di *blk.DataItem, ofblk []util.UID, id int, inputs db.Inputs) error {
// 	return nil
// }

// 	ofblk := make([]*blk.OverflowItem, 2)
// 	ofblk[0].Pkey = v.Encodeb64()
// 	ofblk[0].SortK = pn.SortK + "P" // associated parent node
// 	ofblk[0].B = pn.Pkey            // parent node to which overflow block belongs

// 	ofblk[1].Pkey = v
// 	ofblk[1].SortK = pn.SortK // A#G#:S

// 	const (
// 		Item1 = 0 // Item 1
// 		Item2 = 1 // Item 2
// 	)

// 	Initialise overflow block with two items

// 	Block item 1 - contains a pointer back to the parent ie.Pkey of uid-pred containing the overflow block uid (in Nd)

// 	Block item 2 - an uid-pred Nd equivalent item ie. contains uids of child nodes e.g. sortk A#G#:S#<id>. id=1..n
// 	       each A#G#:S#<id> contains some configured number of uids e.g. 500, 1000 etc.
// 	       The id value is sourced from the parent uid-pred "Id" attribute.

// 	var (
// 		av  map[string]*dynamodb.AttributeValue
// 		err error
// 	)
// 	//
// 	for _, v := range ofblk {

// 		for i := Item1; i <= Item2; i++ {
// 			switch i {

// 			case Item1:
// 				input:=db.NewInput()
// 				input.SetKey(PropagatedTbl,v,"P")
// 				input.AddMember("B", di.Pkey)
// 				inputs.Add(input,I)
// 				// type OverflowI1 struct {
// 				// 	Pkey  []byte
// 				// 	SortK string
// 				// 	B     []byte
// 				// }
// 				// // item to identify parent node to which overflow block belongs
// 				// a := OverflowI1{Pkey: v, SortK: "P", B: di.Pkey}
// 				// av, err = dynamodbattribute.MarshalMap(a)
// 				// if err != nil {
// 				// 	return fmt.Errorf("MakeOverflowBlock %s: %s", "Error: failed to marshal type definition ", err.Error())
// 				// }

// 			case Item2:
// 				av, err = newUIDTarget(v, di.SortK, id,inputs) // e.g. sortk#1 A#G#:S#1
// 				if err != nil {
// 					return err
// 				}
// 				convertSet2list(av)
// 			}

// 			{
// 				t0 := time.Now()
// 				ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
// 					TableName:              aws.String(param.GraphTable),
// 					Item:                   av,
// 					ReturnConsumedCapacity: aws.String("TOTAL"),
// 				})
// 				t1 := time.Now()
// 				syslog(fmt.Sprintf("MakeOverflowBlock: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
// 				if err != nil {
// 					return newDBSysErr("MakeOverflowBlock", "PutItem", err)
// 				}
// 			}
// 		}
// 	}
// 	return nil
// }

// ty.   - type of child? node
// puid - parent node uid
// sortK - uidpred of parent to append value G#:S (sibling) or G#:F (friend)
// value - child value
//func firstPropagationScalarItem(ty blk.TyAttrD, pUID util.UID, sortk, sortK string, tUID util.UID, id int, value interface{}) (int, error) { //, wg ...*sync.WaitGroup) error {

// AddReverseEdge maintains reverse edge from child to parent
// e.g. Ross (parentnode) -> sibling -> Ian (childnode), Ian -> R%Sibling -> Ross
// sortk: e.g. A#G#:S, A#G#:F. Attachment point of paraent to which child data is copied.
//
// query: detach node connected to parent UID as a friend?
// Solution: specify field "BS" and  query condition 'contains(PBS,puid+"f")'          where f is the abreviation for friend predicate in type Person
//           if query errors then node is not attached to that predicate, so nothing to delete
//           if query returns, search returned BS and get tUID ie. BS[0][16:32]  which gives you all you need (puid,tUID) to mark {Nd, XF}as deleted.
//
// db.AddReverseEdge(eventID, seq, cUID, pUID, ptyName, sortK, tUID, &cwg)
// func UpdateReverseEdge(cuid, puid, tUID util.UID, sortk string, batchId int) error {
// 	//
// 	// BS : set of binary values representing puid + tUID + sortk(last entry). Used to determine the tUID the child data saved to.
// 	// PBS : set of binary values representing puid + sortk (last entry). Can be used to quickly access if child is attached to parent
// 	pred := func(sk string) string {
// 		s_ := strings.SplitAfterN(sk, "#", -1) // A#G#:S#:D#3
// 		if len(s_) == 0 {
// 			panic(fmt.Errorf("buildExprValues: SortK of %q, must have at least one # separator", sk))
// 		}
// 		return s_[len(s_)-2][1:] + s_[len(s_)-1]
// 	}
// 	sortk += "#" + strconv.Itoa(batchId)
// 	bs := make([][]byte, 1, 1) // representing a binary set.
// 	bs[0] = append(puid, []byte(tUID)...)
// 	bs[0] = append(bs[0], pred(sortk)...) // D#3
// 	//
// 	//	pbs := make([][]byte, 1, 1) // representing a binary set.
// 	//	pbs[0] = append(puid, pred(sortk)...)
// 	//v2[0] = util.UID(v2[0]).Encodeb64_()
// 	//
// 	upd := expression.Add(expression.Name("BS"), expression.Value(bs))
// 	//	upd = upd.Add(expression.Name("PBS"), expression.Value(pbs))
// 	expr, err := expression.NewBuilder().WithUpdate(upd).Build()
// 	if err != nil {
// 		return newDBExprErr("AddReverseEdge", "", "", err)
// 	}
// 	//
// 	// Marshal primary key
// 	//
// 	Pkey := Pkey{Pkey: cuid, SortK: "R#"}
// 	av, err := dynamodbattribute.MarshalMap(&Pkey)
// 	if err != nil {
// 		return newDBMarshalingErr("AddReverseEdge", cuid.String(), "R#", "MarshalMap", err)
// 	}
// 	//
// 	input := &dynamodb.UpdateItemInput{
// 		Key:                       av,
// 		ExpressionAttributeNames:  expr.Names(),
// 		ExpressionAttributeValues: expr.Values(),
// 		UpdateExpression:          expr.Update(),
// 	}
// 	input = input.SetTableName(param.GraphTable).SetReturnConsumedCapacity("TOTAL")
// 	//
// 	{
// 		t0 := time.Now()
// 		uio, err := dynSrv.UpdateItem(input)
// 		t1 := time.Now()
// 		syslog(fmt.Sprintf("AddReverseEdge: consumed updateitem capacity: %s, Duration: %s\n", uio.ConsumedCapacity, t1.Sub(t0)))
// 		if err != nil {
// 			return newDBSysErr("AddReverseEdge", "UpdateItem", err)
// 		}
// 	}

// 	return nil
// }
