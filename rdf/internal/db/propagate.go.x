package db


import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/GoGraph/tx"
)

// propagateScalar maps a GoGraph type entry to its equivalent database (Dynamodb, Spanner) type.
func propagateScalar(ty blk.TyAttrD, pUID util.UID, sortK string, tUID util.UID, id int, value interface{}) tx.Mutation { //, wg ...*sync.WaitGroup) error {
	// **** where does Nd, XF get updated when in Overflow mode.???
	//
	var (
		lty   string
		sortk string
		err   error
	)

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
		if ty.DT != "Nd" {
			// simple scalar e.g. Age
			lty = "L" + ty.DT
			sortk = sortK + "#:" + ty.C + "#" + strconv.Itoa(id) // TODO: currently ignoring concept of partitioning data within node block. Is that right?
		} else {
			// TODO: can remove this section
			// uid-predicate e.g. Sibling
			lty = "Nd"
			//sortk = "A#G#:" + sortK[len(sortK)-1:] // TODO: currently ignoring concept of partitioning data within node block. Is that right? Fix: this presumes single character short name
			sortk = "A#G#:" + sortK[strings.LastIndex(sortK, ":")+1:]
		}
	}
	//
	// dml 
	//
	txMut := tx.NewMutation(propagatedTbl,pUID,sortk)
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
			txMut.AddMember("LI",  v)
		case int32:
			v := make([]int32, 1, 1)
			v[0] = x
			txMut.AddMember("LI",  v)
		case int64:
			v := make([]int64, 1, 1)
			v[0] = x
			txMut.AddMember("LI",  v)
		case float64:
			v := make([]float64, 1, 1)
			v[0] = x
			txMut.AddMember("LF",  v)
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
			txMut.AddMember(lty, v)
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
			txMut.AddMember(lty, v)
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
			txMut.AddMember(lty, v)
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
			txMut.AddMember(lty,  v)
		}

	}
	//
	// bool represents if entry passed in is defined or not. True means it is not defined equiv to null entry.
	//
	txMut.AddMember("XBl", nil)
	//
	// Marshal primary key of parent node
	//
	return txMut 
}
