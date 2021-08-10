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
