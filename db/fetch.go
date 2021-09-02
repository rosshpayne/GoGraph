package db

import (
	"context"
	"fmt"
	"strings"
	"time"

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
)

type request byte

const (
	scalar         request = 'S'
	edge                   = 'E'
	propagated             = 'P'
	reverse                = 'R'
	overflow               = 'O'
	edgepropagated         = 'D'
)

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

// FetchNode used to fetch Scalar data or Edge data or edge-overflow data as determinded by sortk parameter
func FetchNode(uid util.UID, subKey ...string) (blk.NodeBlock, error) {

	var (
		sortk string
		sql   string
	)
	// type Block struct {
	// 	PKey []byte `spanner:"PKey"`
	// 	// Node
	// 	IsNode byte   // 'y', null if not
	// 	Ty     string // Node type
	// 	// Overflow Block
	// 	P util.UID //  Parent Node
	// 	//Ns    [][]byte ???
	// }
	type Scalar struct {
		PKey  []byte `spanner:"PKey"`
		Sortk string `spanner:"Sortk"`
		Ty    string // parent type
		//Ty string - now in Block

		Bl spanner.NullBool
		S  spanner.NullString
		F  spanner.NullFloat64
		I  spanner.NullInt64
		B  []byte
		DT spanner.NullTime
		//
		LS  []string
		LI  []int64
		LF  []float64
		LBl []bool
		LB  [][]byte
		LDT []time.Time
		//
		// SS []string
		// NS []int64
		// BS [][]byte
	}
	//type Edges struct {
	// Edge-Overflow-Propagated data
	type Edge struct {
		PKey  []byte `spanner:"PKey"`
		Sortk string `spanner:"Sortk"`
		Ty    string // parent type
		//
		Nd [][]byte
		Id []int64
		XF []int64
	}

	type Overflow struct {
		PKey  []byte `spanner:"PKey"`
		Sortk string `spanner:"Sortk"`
		Ty    string `spanner:"Ty"`
		P     []byte `spanner:"P"` // parent UID
		//
		Nd [][]byte
		XF []int64
		// P, Ty, N (see SaveUpredState) ???
		LS  []string
		LI  []int64
		LF  []float64
		LBl []bool
		LDT []time.Time
		LB  [][]byte
		// determines if slice entry is null (true), default false
		XBl []bool
	}

	type Propagated struct {
		PKey  []byte `spanner:"PKey"`
		Sortk string `spanner:"Sortk"`
		Ty    string // parent type
		// P, Ty, N (see SaveUpredState) ???
		LS  []string
		LI  []int64
		LF  []float64
		LBl []bool
		LDT []time.Time
		LB  [][]byte
		// determines if slice entry is null (true), default false
		XBl []bool
	}

	type Reverse struct {
		PKey  []byte `spanner:"PKey"` // child UID
		Sortk string `spanner:"Sortk"`
		Ty    string // parent type
		B     []byte // P value ie. Parent UID
		//
		Puid  []byte
		Ouid  []byte
		Batch spanner.NullInt64
	}

	ctx := context.Background()

	if len(subKey) > 0 {
		sortk = subKey[0]
		slog.Log("DB FetchNode: ", fmt.Sprintf(" node: %s subKey: %s", uid.String(), sortk))
	} else {
		sortk = "A#A#"
		slog.Log("DB FetchNode: ", fmt.Sprintf(" node: %s subKey: %s", uid.String(), sortk))
	}
	//defer client.Close()

	// stmt returns one row

	fetchType := func() request {
		switch sortk {
		case "A#A#", "A#B#", "A#C#", "A#D#", "A#E#", "A#F#":
			return scalar
		default:
			switch {
			case strings.HasPrefix(sortk, "A#G#"):
				switch strings.Count(sortk, "#") {
				case 2: // "A#G#:?"
					return edge
				case 3: // "A#G#:?#:?"
					return propagated
				}
			case strings.HasPrefix(sortk, "ALL"):
				sortk = sortk[3:]
				return edgepropagated

			case strings.HasPrefix(sortk, "OV"): //TODO: prex
				return overflow

			case strings.HasPrefix(sortk, "R#"):
				return reverse
			}
		}
		panic(fmt.Errorf("Error in db.FetchNode: fetchType not determined based on sortk of %q", sortk))
	}

	params := map[string]interface{}{"uid": []byte(uid), "sk": sortk}

	switch fetchType() {
	case scalar: // SortK: A#A#
		sql = `Select n.PKey, n.Ty, ns.SortK, ns.S, ns.I, ns.F, ns.Bl, ns.B, ns.DT, ns.LI, ns.LF, ns.LBl, ns.LB, ns.LDT
				from Block n 
				join NodeScalar ns using (PKey)
				where n.Pkey = @uid and  Starts_With(ns.Sortk,@sk)`
	case edge: // UID-PRED SortK: A#G#:?
		// used by attach node to determine target UID for propatated data. Only edge data required, hence this query.
		sql = `Select n.PKey, e.Sortk, n.Ty, e.XF, e.Id, e.Nd
				from Block n 
				join EOP e using (PKey)
				where n.Pkey = @uid and  e.Sortk = @sk`
	case propagated: // SortK: A#G#:?#
		// used by query execute to query propagated data (each array type represening a columnar format)
		sql = `Select n.PKey, n.Ty, ps.SortK, ps.LI, ps.LF, ps.LBl, ps.LB, ps.LDT, ps.LS
				from Block n 
				join EOP ps using (PKey)
				where n.Pkey = @uid and  Starts_With(ps.Sortk,@sk)`
	case edgepropagated: // UID-PRED SortK: A#G#:? + propagated data
		// used by attach node to determine target UID for propatated data. Only edge data required, hence this query.
		sql = `Select n.PKey, e.Sortk, n.Ty, e.XF, e.Id, e.Nd
				from Block n 
				join EOP e using (PKey)
				where n.Pkey = @uid and Starts_With(ps.Sortk,@sk)`
	case overflow: // SortK: A#O#:? and A#O#:?#?@
		// used by unmarshalNodeCache (block->cache)
		sql = `Select n.PKey, o.Sortk, n.Ty, o.XF, o.Nd , o.SortK, o.LS, o.LI, o.LF, o.LBl, o.LB, o.LDT
				from Block n 
				join EOP o using (PKey)
				where n.Pkey = @ouid and  Starts_With(o.Sortk,@sk)`
	case reverse: // SortK: R#
		sql = `Select n.PKey, r.Sortk, r.pUID, r.Batch
				from Block n 
				join Reverse r using (PKey)
				where n.Pkey = @uid`
	}

	// sql := `Select PKey,"A#A#T" SortK, Ty, P,
	// 				ARRAY (select  PKey from NodeScalar       ns where ns.PKey = @uid and  Starts_With(ns.Sortk,@sk)) as ns
	// 				ARRAY (select as struct * from Edge             e  where  e.PKey = @uid and  Starts_With(e.Sortk,@sk)) as e,
	// 				ARRAY (select as struct * from PropagatedScalar ps where ps.PKey = @uid and  Starts_With(ps.Sortk,@sk)) as ps
	// 		   from Block n
	// 		   where n.Pkey = @uid`

	iter := client.Single().Query(ctx, spanner.Statement{SQL: sql, Params: params})

	var (
		nb blk.NodeBlock
	)

	switch fetchType() {

	case scalar:

		first := true
		err = iter.Do(func(r *spanner.Row) error {
			// for each row - however only one row is return from db.
			//
			// Unmarshal database output into Bdi
			//
			rec := Scalar{}
			err := r.ToStruct(&rec)
			if err != nil {
				fmt.Println("ToStruct error: %s", err.Error())
				return err
			}
			if first {
				nbrow := &blk.DataItem{}
				nbrow.Pkey = rec.PKey
				nbrow.Sortk = "A#A#T"
				nbrow.Ty = rec.Ty
				nb = append(nb, nbrow)
				first = false
			}
			//
			nbrow := &blk.DataItem{}
			nbrow.Pkey = rec.PKey
			nbrow.Sortk = rec.Sortk
			switch {
			case !rec.S.IsNull():
				nbrow.S = rec.S.StringVal
			case !rec.I.IsNull():
				nbrow.I = rec.I.Int64
			case !rec.F.IsNull():
				nbrow.F = rec.F.Float64
			case !rec.DT.IsNull():
				nbrow.DT = rec.DT.Time
			case !rec.Bl.IsNull():
				nbrow.Bl = rec.Bl.Bool
			}
			nbrow.B = rec.B
			nbrow.LS = rec.LS
			nbrow.LI = rec.LI
			nbrow.LF = rec.LF
			nbrow.LB = rec.LB
			nbrow.LBl = rec.LBl
			nbrow.LDT = rec.LDT
			//
			nb = append(nb, nbrow)

			return nil
		})

	case edge:

		first := true
		err = iter.Do(func(r *spanner.Row) error {

			rec := Edge{}
			err := r.ToStruct(&rec)
			if err != nil {
				fmt.Println("ToStruct error: %s", err.Error())
				return err
			}
			if first {
				nbrow := &blk.DataItem{}
				nbrow.Pkey = rec.PKey
				nbrow.Sortk = "A#A#T"
				nbrow.Ty = rec.Ty
				nb = append(nb, nbrow)
				first = false
			}
			nbrow := &blk.DataItem{}
			nbrow.Pkey = rec.PKey
			nbrow.Sortk = rec.Sortk
			nbrow.Nd = rec.Nd
			nbrow.XF = rec.XF
			nbrow.Id = rec.Id

			nb = append(nb, nbrow)

			return nil
		})

	case propagated:

		first := true
		err = iter.Do(func(r *spanner.Row) error {
			rec := Propagated{}
			err := r.ToStruct(&rec)
			if err != nil {
				fmt.Println("ToStruct error: %s", err.Error())
				return err
			}
			if first {
				nbrow := &blk.DataItem{}
				nbrow.Pkey = rec.PKey
				nbrow.Sortk = "A#A#T"
				nbrow.Ty = rec.Ty
				nb = append(nb, nbrow)
				first = false
			}

			nbrow := &blk.DataItem{}
			nbrow.Pkey = rec.PKey
			nbrow.Sortk = rec.Sortk
			nbrow.LS = rec.LS
			nbrow.LI = rec.LI
			nbrow.LF = rec.LF
			nbrow.LBl = rec.LBl
			nbrow.LDT = rec.LDT
			nbrow.LB = rec.LB
			nbrow.XBl = rec.XBl

			nb = append(nb, nbrow)

			return nil
		})

	case overflow:

		first := true
		err = iter.Do(func(r *spanner.Row) error {
			rec := Overflow{}
			err := r.ToStruct(&rec)
			if err != nil {
				fmt.Println("ToStruct error: %s", err.Error())
				return err
			}
			if first {
				nbrow := &blk.DataItem{}
				nbrow.Pkey = rec.PKey // Ouid
				nbrow.Sortk = "A#A#T"
				nbrow.Ty = rec.Ty
				nb = append(nb, nbrow)
				//
				nbrow = &blk.DataItem{}
				nbrow.Pkey = rec.PKey // Ouid
				nbrow.Sortk = "P"
				nbrow.B = rec.P
				//
				first = false
			}
			nbrow := &blk.DataItem{}
			// Overflow batches
			if len(rec.Nd) > 0 {
				nbrow.Pkey = rec.PKey
				nbrow.Sortk = rec.Sortk
				nbrow.Nd = rec.Nd
				nbrow.XF = rec.XF
			} else {
				nbrow := &blk.DataItem{}
				nbrow.Pkey = rec.PKey
				nbrow.Sortk = rec.Sortk
				nbrow.LS = rec.LS
				nbrow.LI = rec.LI
				nbrow.LF = rec.LF
				nbrow.LBl = rec.LBl
				nbrow.LDT = rec.LDT
				nbrow.LB = rec.LB
				nbrow.XBl = rec.XBl
			}
			nb = append(nb, nbrow)

			return nil
		})

	case reverse:

		first := true
		err = iter.Do(func(r *spanner.Row) error {
			rec := Reverse{}
			err := r.ToStruct(&rec)
			if err != nil {
				fmt.Println("ToStruct error: %s", err.Error())
				return err
			}
			if first {
				nbrow := &blk.DataItem{}
				nbrow.Pkey = rec.PKey
				nbrow.Sortk = "A#A#T"
				nbrow.Ty = rec.Ty
				nb = append(nb, nbrow)
				first = false
			}
			nbrow := &blk.DataItem{}
			nbrow.Pkey = rec.PKey
			nbrow.Sortk = rec.Sortk
			nbrow.LB = make([][]byte, 2, 2)
			nbrow.LB[0] = rec.Puid
			nbrow.LB[1] = rec.Ouid
			if !rec.Batch.IsNull() {
				nbrow.I = rec.Batch.Int64
			}

			nb = append(nb, nbrow)

			return nil
		})
	}
	if err != nil {
		fmt.Println("=== error in Query ====")
		panic(err)
	}
	// fmt.Printf("child nb: len %d \n", len(nb))
	// for _, v := range nb {
	// 	fmt.Printf("data: %#v\n", *v)
	// }

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

//TODO: replace this function with the one above. Unnecessary to have two.
func FetchNodeItem(uid util.UID, sortk string) (blk.NodeBlock, error) {
	return FetchNode(uid, sortk)
}
