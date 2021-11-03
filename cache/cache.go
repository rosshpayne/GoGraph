package cache

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"sync"
	//"time"

	blk "github.com/GoGraph/block"
	"github.com/GoGraph/ds"
	param "github.com/GoGraph/dygparam"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/types"
	"github.com/GoGraph/util"
)

func syslog(s string) {
	slog.Log("Cache: ", s)
}

// errors
var ErrCacheEmpty = errors.New("Cache is empty")

//  ItemCache struct is the transition between Dynamodb types and the actual attribute type defined in the DD.
//  Number (dynamodb type) -> float64 (transition) -> int (internal app & as defined in DD)
//  process: dynamodb -> ItemCache -> DD conversion if necessary to application variables -> ItemCache -> Dynamodb
//	types that require conversion from ItemCache to internal are:
//   DD:   int         conversion: float64 -> int
//   DD:   datetime    conversion: string -> time.Time
//  all the other datatypes do not need to be converted.

type SortKey = string

// type NodeCache struct {
// 	m map[SortKey]*blk.DataItem
// 	sync.Mutex
// }

//type block map[SortKey]*blk.DataItem

// ************************************ Node cache ********************************************

// data associated with a single node
type NodeCache struct {
	sync.RWMutex // used for querying the cache data items. Promoted methods RLock(), Unlock()
	m            map[SortKey]*blk.DataItem
	Uid          util.UID    // TODO - should this be exposed
	fullLock     bool        // true for Lock, false for read Lock
	gc           *GraphCache // point back to graph-cache
}

type entry struct {
	ready chan struct{} // a channel for each entry - to synchronise access when the data is being sourced
	*NodeCache
}
type Rentry struct {
	ready chan struct{} // a channel for each entry - to synchronise access when the data is being sourced
	sync.RWMutex
}

// graph cache consisting of all nodes loaded into memory
type GraphCache struct {
	sync.RWMutex
	cache  map[util.UIDb64s]*entry
	rsync  sync.RWMutex
	cacheR map[util.UIDb64s]*Rentry // not used?
}

var graphC GraphCache

func NewCache() *GraphCache {
	return &graphC
}

func GetCache() *GraphCache {
	return &graphC
}

func (n *NodeCache) GetMap() map[SortKey]*blk.DataItem {
	return n.m
}

// ====================================== init =====================================================

func init() {
	// cache of nodes
	graphC = GraphCache{cache: make(map[util.UIDb64s]*entry)}
	//
	//FacetC = make(map[types.TyAttr][]FacetTy)
}

func (g *GraphCache) IsCached(uid util.UID) (ok bool) {
	g.Lock()
	_, ok = g.cache[uid.String()]
	g.Unlock()
	return ok
}

func (np *NodeCache) GetOvflUIDs(sortk string) []util.UID {
	// TODO: replace A#G#:S" with generic term
	// get np.uidPreds
	if di, ok := np.m[sortk]; ok { // np.GetDataItem("A#G#:S"); ok {
		_, _, oUID := di.GetNd()
		ids := len(oUID)
		if ids > 0 {
			m := make([]util.UID, ids, ids)
			for i, v := range oUID {
				m[i] = util.UID(v)
			}
			return m
		}
	}
	return nil
}

func (n *NodeCache) GetDataItem(sortk string) (*blk.DataItem, bool) {
	if x, ok := n.m[sortk]; ok {
		return x, ok
	}
	return nil, false
}

var NoNodeTypeDefinedErr error = errors.New("No type defined in node data")

type NoTypeDefined struct {
	ty string
}

func (e NoTypeDefined) Error() string {
	return fmt.Sprintf("Type %q not defined", e.ty)
}

// genSortK, called from query component (execute.go) to determine how to best to query a node's data based on
// the subsection of the statement's (represented by the NV) being executed.
// It may produce more than on Sortk key requireing multiple datbase ios.
func GenSortK(nvc ds.ClientNV, ty string) []string {
	//genSortK := func(attr string) (string, bool) {
	var (
		ok                    bool
		sortkS                []string
		aty                   blk.TyAttrD
		scalarPreds, uidPreds int
	)
	//
	// count predicates, scalar & uid.
	// ":" used to identify uid-preds
	//
	if len(ty) == 0 {
		panic(fmt.Errorf("Error in GenSortK: argument ty is empty"))
	}
	for _, nv := range nvc {
		if strings.IndexByte(nv.Name, ':') == -1 {
			scalarPreds++
		} else {
			// includes uidpreds the progated data of its child nodes (all stored in  table EOP)
			uidPreds++
		}
	}
	//
	// get type info
	//
	// if tyc, ok :=  types.TypeC.TyC[ty]; !ok {
	// 	panic(fmt.Errorf(`genSortK: Type %q does not exist`, ty))
	// }
	// get long type name
	ty, _ = types.GetTyLongNm(ty)
	var s strings.Builder

	switch {

	case scalarPreds == 1 && uidPreds == 0:
		s.WriteString(types.GraphSN())
		s.WriteByte('|')
		s.WriteString("A#")
		if aty, ok = types.TypeC.TyAttrC[ty+":"+nvc[0].Name]; !ok {
			panic(fmt.Errorf("Predicate %q does not exist in type %q", nvc[0].Name, ty))
		} else {
			s.WriteString(aty.P)
			s.WriteString("#:")
			s.WriteString(aty.C)
		}
		sortkS = append(sortkS, s.String())

	case scalarPreds > 1 && uidPreds == 0:
		// for each scalar partition assign a sortk and query separately
		var parts map[string]bool

		parts = make(map[string]bool)
		for i, nv := range nvc {
			if aty, ok = types.TypeC.TyAttrC[ty+":"+nv.Name]; !ok {
				panic(fmt.Errorf("Predicate %q does not exist in type %q", nvc[i].Name, ty))
			} else {
				if !parts[aty.P] {
					parts[aty.P] = true
				}
			}
		}
		// a fetch per partition
		for k, _ := range parts {
			s.WriteString(types.GraphSN())
			s.WriteByte('|')
			s.WriteString("A#")
			s.WriteString(k)
			sortkS = append(sortkS, s.String())
			s.Reset()
		}

	case uidPreds == 1 && scalarPreds == 0:
		s.WriteString(types.GraphSN())
		s.WriteByte('|')
		s.WriteString("A#")
		if aty, ok = types.TypeC.TyAttrC[ty+":"+nvc[0].Name]; !ok {
			panic(fmt.Errorf("Predicate %q does not exist in type %q", nvc[0].Name, ty))
		} else {
			s.WriteString("G#:")
			s.WriteString(aty.C)
		}
		sortkS = append(sortkS, s.String())

	case uidPreds > 1 && scalarPreds == 0:
		s.WriteString(types.GraphSN())
		s.WriteByte('|')
		s.WriteString("A#G#")
		sortkS = append(sortkS, s.String())

	default:
		// case uidPreds > 1 && scalarPReds > 1:
		// TODO: should this be decomposed into multi-partition fetches?? NO benefit I think.
		s.WriteString(types.GraphSN())
		s.WriteByte('|')
		s.WriteString("A#")
		sortkS = append(sortkS, s.String())

	}
	//
	return sortkS
}

func (nc *NodeCache) UnmarshalCache(nv ds.ClientNV) error {
	return nc.UnmarshalNodeCache(nv)
}

// UnmarshalCache, unmarshalls the nodecache containing into the value attribute of ds.ClientNV derived from the query statement.
// currently it presumes all propagated scalar data must be prefixed with A#.
// need to have locked the data for the duration of the unmarshal operation - to prevent any concurrent Updates on the data.
// TODO: extend to include G# prefix.
// nc must have been acquired using either
// * FetchForUpdaate(uid)
// * FetchNode(uid)
//
// Type differences between query and data.
// ----------------------------------------
// NV is generated from the query statement which is usually based around around a known type.
// Consequently, NV.Name is based the predicates in the known type.
// However the results from the root query don't necessarily have to match the type used to define the query.
// When the types differ only those predicates that match (based on predicate name - NV.Name) can be unmarshalled.
// ty_ should be the type of the item resulting from the root query which will necessarily match the type from the item cache.
// If ty_ is not passed then the type is sourced from the cache, at the potental cost of a read, so its better to pass the type if known
// which should always be the case.
//
func (nc *NodeCache) UnmarshalNodeCache(nv ds.ClientNV, ty_ ...string) error {
	if nc == nil {
		return ErrCacheEmpty
	}
	var (
		sortk  string
		attrDT string
		ok     bool
		// sortk2  string
		// attrDT2 string
		// ok2     bool
		attrKey string
		ty      string // short name for item type e.g. Pn (for Person)

		err error
	)

	// root type to which attributes belong
	if len(ty_) > 0 {
		ty = ty_[0]
	} else {
		if ty, ok = nc.GetType(); !ok {
			return NoNodeTypeDefinedErr
		}
	}
	// if ty is short name convert to long name
	if x, ok := types.GetTyLongNm(ty); ok {
		ty = x
	}
	// current Type (long name)
	//fmt.Println("UnmarshalNodeCache  ty: ", ty)
	// types.FetchType populates  struct cache.TypeC with map types TyAttr, TyC
	if _, err = types.FetchType(ty); err != nil {
		return err
	}

	// GetCachedNode gets node from cache. This call presumes node has already been loaded into cache otherwise returns error
	GetNode := func(uid util.UID) *NodeCache {

		uids := uid.String()

		graphC.Lock()
		e := graphC.cache[uids]
		graphC.Unlock()

		if e == nil {
			panic(errors.New("System Error: in UnmarshalNodeCache, GetNode() node not found in cache"))
		}
		// lock held by FetchUOB
		return e.NodeCache
	}

	genSortK := func(attr string) (string, string, bool) {
		var (
			pd     strings.Builder
			aty    blk.TyAttrD
			attrDT string
			ok     bool
		)
		// Scalar attribute
		attr_ := strings.Split(attr, ":")
		ty := ty // start with query root type
		pd.WriteString(types.GraphSN())
		pd.WriteByte('|')
		pd.WriteString("A#") // leading partition
		c := 1
		colons := strings.Count(attr, ":")
		for _, j := range attr_ {
			if len(j) == 0 {
				// break on a UID-PRED "Siblings:"
				break
			}
			if aty, ok = types.TypeC.TyAttrC[ty+":"+j]; !ok {
				return "", "", false
			}
			attrDT = aty.DT // Data Type
			// uid-predicate:
			// one promote - child.updpred:scalar
			// two promotes - child.updpred:grandchild.uidpred:scalar
			switch colons {
			case 0:
				// scalar
				pd.WriteString(aty.P) // subPartition off leading partition e.g. "A#A"
				pd.WriteString("#:")
				pd.WriteString(aty.C) // short (Compressed) attribute name
			case 1:
				// UID-PRED attribute and Scalar for child node e.g. Friend:, Sibling:, Friend:DOB
				if aty.DT != "Nd" {
					attrDT = "UL" + aty.DT
				}
				// single promote of scalar
				// scalar only
				switch c {
				case 1:
					pd.WriteString("G#:")
					pd.WriteString(aty.C)
					c++
				case 2:
					pd.WriteString("#:")
					pd.WriteString(aty.C)
				}
			case 2:
				// double promote of scalars
				// uid-preds only
				if aty.DT != "Nd" {
					attrDT = "UL" + aty.DT
				}
				switch c {
				case 1:
					pd.WriteString("G#:")
					pd.WriteString(aty.C)
					c++
				case 2:
					pd.WriteString("#G#:")
					pd.WriteString(aty.C)
					c++
				case 3:
					pd.WriteString("#:")
					pd.WriteString(aty.C)
				}
			}
			if len(aty.Ty) > 0 {
				// new UID-PRED: change current node type e.g. Person
				ty = aty.Ty
			}

		}
		return pd.String(), attrDT, true
	}
	// This data is stored in uid-pred UID item that needs to be assigned to each child data item
	var State [][]int64
	var oUIDs [][]byte

	sortK := func(key string, i int) string {
		var s strings.Builder
		s.WriteString(key)
		s.WriteByte('%')
		s.WriteString(strconv.Itoa(i)) // batch Id 1..n
		return s.String()
	}
	// &ds.NV{Name: "Age"},
	// &ds.NV{Name: "Name"},
	// &ds.NV{Name: "DOB"},
	// &ds.NV{Name: "Cars"},
	// &ds.NV{Name: "Siblings:"},     <== Nd type is defined before refering to its attributes
	// &ds.NV{Name: "Siblings:Name"}, <=== propagated child scalar data
	// &ds.NV{Name: "Siblings:Age"},  <=== propagated child scalar data

	for _, a := range nv { // a.Name = "Age"
		//
		// field name repesents a scalar. It has a type that we use to generate a sortk <partition>#G#:<uid-pred>#:<scalarpred-type-abreviation>
		//
		if sortk, attrDT, ok = genSortK(a.Name); !ok {
			// no match between NV name and type attribute name
			continue
		}
		//fmt.Println("UnmarshalNodeCache: a.Name, sortk, attrDT: ", a.Name, sortk, attrDT)
		//
		// grab the *blk.DataItem from the cache for the nominated sortk.
		// we could query the child node to get this data or query the #G data which is its copy of the data
		//
		a.ItemTy = ty // root node type or uid-pred type
		attrKey = sortk
		//
		if v, ok := nc.m[sortk]; ok {
			// based on data type and whether its a node or uid-pred
			switch attrDT {
			//
			// Scalars
			//
			case "I": // int
				a.Value = v.GetI()
			case "F": // float
				a.Value = v.GetF()
			case "S": // string
				a.Value = v.GetS()
			case "Bl": // bool
				a.Value = v.GetBl()
			case "DT": // DateTime - stored as string
				a.Value = v.GetDT()

			// Scalar Sets
			// case "IS": // set int
			// 	a.Value = v.GetIS()
			// case "FS": // set float
			// 	a.Value = v.GetFS()
			// case "SS": // set string
			// 	a.Value = v.GetSS()
			// case "BS": // set binary
			// 	a.Value = v.GetBS()

			// Scalar Lists
			case "LS": // list string
				a.Value = v.GetLS()
			case "LF": // list float
				a.Value = v.GetLF()
			case "LI": // list int
				a.Value = v.GetLI()
			case "LB": // List []byte
				a.Value = v.GetLB()
			case "LBl": // List bool
				a.Value = v.GetLBl()
			//
			// Propagated Scalars...
			//
			case "ULS": // list string
				//a.Value = v.GetLBl()
				var allLS [][]string
				var allXbl [][]bool
				// data from root uid-pred block
				ls, xbl := v.GetULS()

				allLS = append(allLS, ls)
				allXbl = append(allXbl, xbl)
				// data from overflow blocks
				for _, v := range oUIDs {
					// Fetches from cache - as FetchUOB has loaded OBlock into cache
					nuid := GetNode(util.UID(v))
					// iterate over all overflow items in the overflow block for key attrKey
					for i := 1; true; i++ {
						if di, ok := nuid.m[sortK(attrKey, i)]; !ok {
							break //return fmt.Errorf("UnmarshalCache: SortK %q does not exist in cache", attrKey)
						} else {
							ls, xbl := di.GetULS()
							allLS = append(allLS, ls)    //ls[1:])
							allXbl = append(allXbl, xbl) //xbl[1:])
						}
					}
				}
				a.Value = allLS
				a.Null = allXbl
				a.State = State
				a.OfUIDs = oUIDs

			case "ULF": // list float
				//a.Value = v.GetLBl()
				var allLF [][]float64
				var allXbl [][]bool
				// data from root uid-pred block
				lf, xbl := v.GetULF()

				allLF = append(allLF, lf)
				allXbl = append(allXbl, xbl)
				// data from overflow blocks
				for _, v := range oUIDs {
					// Fetches from cache - as FetchUOB has loaded OBlock into cache
					nuid := GetNode(util.UID(v))
					// iterate over all overflow items in the overflow block for key attrKey
					for i := 1; true; i++ {
						if di, ok := nuid.m[sortK(attrKey, i)]; !ok {
							break //return fmt.Errorf("UnmarshalCache: SortK %q does not exist in cache", attrKey)
						} else {
							lf, xbl := di.GetULF()
							allLF = append(allLF, lf)    //lb[1:])
							allXbl = append(allXbl, xbl) //xbl[1:])
						}
					}
				}
				a.Value = allLF
				a.Null = allXbl
				a.State = State
				a.OfUIDs = oUIDs

			case "ULI": // list int

				var allLI [][]int64
				var allXbl [][]bool
				// data from root uid-pred block
				li, xbl := v.GetULI()

				allLI = append(allLI, li)
				allXbl = append(allXbl, xbl)
				// data from overflow blocks
				for _, v := range oUIDs {
					// Fetches from cache - as FetchUOB has loaded OBlock into cache
					nuid := GetNode(util.UID(v))
					// iterate over all overflow items in the overflow block for key attrKey
					for i := 1; true; i++ {
						if di, ok := nuid.m[sortK(attrKey, i)]; !ok {
							break //return fmt.Errorf("UnmarshalCache: SortK %q does not exist in cache", attrKey)
						} else {
							li, xbl := di.GetULI()
							allLI = append(allLI, li)    //li[1:])
							allXbl = append(allXbl, xbl) //xbl[1:])
						}
					}
				}
				a.Value = allLI
				a.Null = allXbl
				a.State = State
				a.OfUIDs = oUIDs

			case "ULB": // List []byte

				var allLB [][][]byte
				var allXbl [][]bool
				// data from root uid-pred block
				lb, xbl := v.GetULB()

				allLB = append(allLB, lb)
				allXbl = append(allXbl, xbl)
				// data from overflow blocks
				for _, v := range oUIDs {
					// Fetches from cache - as FetchUOB would have loaded OBlock into cache
					nuid := GetNode(util.UID(v))
					for i := 1; true; i++ {
						if di, ok := nuid.m[sortK(attrKey, i)]; !ok {
							break //return fmt.Errorf("UnmarshalCache: SortK %q does not exist in cache", attrKey)
						} else {
							lb, xbl := di.GetULB()
							allLB = append(allLB, lb)    //lb[1:])
							allXbl = append(allXbl, xbl) //xbl[1:])
						}
					}
				}
				a.Value = allLB
				a.Null = allXbl
				a.State = State
				a.OfUIDs = oUIDs

			case "ULBl": // List bool
				//a.Value = v.GetLBl()
				var allLBl [][]bool
				var allXbl [][]bool
				// data from root uid-pred block
				bl, xbl := v.GetULBl()

				allLBl = append(allLBl, bl)
				allXbl = append(allXbl, xbl)
				// data from overflow blocks
				for _, v := range oUIDs {
					// Fetches from cache - as FetchUOB has loaded OBlock into cache
					nuid := GetNode(util.UID(v))
					for i := 1; true; i++ {
						if di, ok := nuid.m[sortK(attrKey, i)]; !ok {
							break //return fmt.Errorf("UnmarshalCache: SortK %q does not exist in cache", attrKey)
						} else {
							bl, xbl := di.GetULBl()
							allLBl = append(allLBl, bl)  //bl[1:])
							allXbl = append(allXbl, xbl) //xbl[1:])
						}
					}
				}
				a.Value = allLBl
				a.Null = allXbl
				a.State = State
				a.OfUIDs = oUIDs

			case "Nd": // uid-pred cUID or OUID + XF data
				var (
					allcuid [][][]byte
					xfall   [][]int64
					//
					wg   sync.WaitGroup
					ncCh chan *NodeCache
				)
				// read root UID-PRED (i.e. "Siblings") edge data counting Child nodes and any overblock UIDs
				cuid, xf, oUIDs := v.GetNd()
				ncCh = make(chan *NodeCache)
				// share oUIDs amoungst all propgatated data types
				// if len(oUIDs) > 0 {
				// 	// assign local oUIDs to function scope oUIDs
				// 	//oUIDs = oUIDs
				// 	// setup concurrent reads of UUID batches
				// 	limiter = grmgr.New("Of", 6)
				// }
				//  else {
				// 	oUIDs = oUIDs
				// }
				allcuid = append(allcuid, cuid) // ignore dummy entry
				xfall = append(xfall, xf)       // ignore dummy entry

				var wgOuid sync.WaitGroup
				wgOuid.Add(len(oUIDs))
				// db fetch UID-PRED (Nd, XF) and []scalar data from overflow blocks
				if len(oUIDs) > 0 {

					// read overflow blocks concurrently
					go func() {

						for _, v := range oUIDs {

							go nc.gc.FetchUOB(util.UID(v), &wg, ncCh)

						}
						wgOuid.Wait()
						close(ncCh) // End-of-UOBs
					}()

					// read overflow cache map from channel (as generated by db.FetchNode) - ncm: node cache map [sortk]*blk.DataItem
					for ncm := range ncCh {

						for i := 1; true; i++ {

							if di, ok := ncm.m[sortK(attrKey, i)]; !ok {

								break // no more UUID batches
							} else {
								uof, xof := di.GetOfNd()
								// check if target item is populated. Note: #G#:S#1 will always contain atleast one cUID but #G#:S#2 may not contain any.
								// this occurs as UID item target is created as item id is incremented but associated scalar data target items are created on demand.
								// so a UID target item may exist without any associated scalar data targets. Each scalar data target items will always contain data associated with each cUID attached to parent.
								if len(uof) > 0 {
									allcuid = append(allcuid, uof) // ignore first entry
									xfall = append(xfall, xof)     // ignore first entry
								}
							}
						}
						defer ncm.Unlock()
					}
				}

				a.Value = allcuid
				a.State = xfall
				// share state amongst all propgated datat type
				State = xfall

			default:
				panic(fmt.Errorf("Unsupported data type %q", attrDT))
			}
		}
	}

	return nil

}

// channel payload
type BatchPy struct {
	Bid   int // block id. embedded (0) and overflow (1..n)
	Batch int // overflow batch id. 1..n
	Puid  util.UID
	// overflow batch channel
	DI *blk.DataItem
}

type BatchChs []chan BatchPy

func (nc *NodeCache) UnmarshalEdge(sortk string) BatchChs {

	var (
		bChs BatchChs
	)
	if nc == nil {
		panic(ErrCacheEmpty)
	}
	syslog(fmt.Sprintf("UnmarshalEdge: %s sortk %s  len(nc.m) %d", nc.Uid, sortk, len(nc.m)))
	if v, ok := nc.m[sortk]; ok {
		// based on data type and whether its a node or uid-pred
		var (
			wg sync.WaitGroup
		//	limiter *grmgr.Limiter
		)
		// read root UID-PRED (i.e. "Siblings") edge data counting Child nodes and any overblock UIDs
		cuid, xf, oUIDs := v.GetNd()
		// batch ids for each overflow block
		id := v.GetId()[len(cuid):]
		// create a channel for each overflow block plus parent node (for edge of interest)
		bChs = make(BatchChs, len(oUIDs)+1)
		for i := 0; i < len(oUIDs)+1; i++ {
			if i == 0 {
				// no buffer - channel 0 used to sync with dp read
				bChs[i] = make(chan BatchPy)
			} else {
				bChs[i] = make(chan BatchPy, 2)
			}
		}
		syslog(fmt.Sprintf("UnmarshalEdge %s  v.GetNd() -> cuid, len(cuid) %d len(xf) %d, len(oUIDs) %d", nc.Uid, len(cuid), len(xf), len(oUIDs)))
		// a channel for each overflow block
		//limiter = grmgr.New("edgeCh", len(oUIDs))
		// read overflow blocks concurrently
		go func() {

			// current node's UID-PRED attribute
			bChs[0] <- BatchPy{Bid: 0, Puid: nc.Uid, DI: v}
			close(bChs[0])
			// now for all overflow blocks

			for i, u := range oUIDs {

				// limiter.Ask()
				// <-limiter.RespCh()
				i := i
				wg.Add(1)
				syslog(fmt.Sprintf("About to go nc.gc.FetchBatc : %d", i))
				go nc.gc.FetchBatch(i+1, id[i], u, &wg, sortk, bChs[i+1])

			}
			wg.Wait()
			//
			syslog("all batch goroutines finished....")

		}()

	} else {
		panic(fmt.Errorf("Errror in UnmarshalEdge Sortk %q not found in node cache map", sortk))
	}

	return bChs

}

func (d *NodeCache) UnmarshalValue(attr string, i interface{}) error {
	if d == nil {
		return ErrCacheEmpty
	}
	var (
		aty blk.TyAttrD
		ty  string
		ok  bool
	)

	if reflect.ValueOf(i).Kind() != reflect.Ptr {
		panic(fmt.Errorf("passed in value must be a pointer"))
	}

	if ty, ok = d.GetType(); !ok {
		return NoNodeTypeDefinedErr
	}
	if _, err := types.FetchType(ty); err != nil {
		return err
	}

	if aty, ok = types.TypeC.TyAttrC[ty+":"+attr]; !ok {
		panic(fmt.Errorf("Attribute %q not found in type %q", attr, ty))
	}
	// build a item clause
	var pd strings.Builder
	// pd.WriteString(aty.P) // item partition
	// pd.WriteByte('#')
	pd.WriteString("A#:") // scalar data
	pd.WriteString(aty.C) // attribute compressed identifier

	for _, v := range d.m {
		// match attribute descriptor
		if v.Sortk == pd.String() {
			// we now know the attribute data type, populate interface value with attribute data
			switch aty.DT {
			case "I":
				if reflect.ValueOf(i).Elem().Kind() != reflect.Int {
					return fmt.Errorf("Input type does not match data type")
				}
				reflect.ValueOf(i).Elem().SetInt(v.GetI())
				//
				// non-reflect version below - does not work as fails to set i to value
				// must return i to work. So reflect is more elegant solution as it does an inplace set.
				// if _,ok := i.(*int); !ok {
				// 	return fmt.Errorf("Input type does not match data type")
				// } // or
				// switch i.(type) {
				// case *int, *int64:
				// default:
				// 	return fmt.Errorf("Input type does not match data type")
				// }
				// ii := v.GetI()
				// fmt.Println("Age: ", ii)
				// i = &ii
				return nil
			default:
				return fmt.Errorf("Input type does not match data type")
			}

		}
	}
	return fmt.Errorf("%s not found in data", attr)

}

// UnmarshalMap is an exmaple of reflect usage. Not used in main program.
func (d *NodeCache) UnmarshalMap(i interface{}) error {
	if d == nil {
		return ErrCacheEmpty
	}
	defer d.Unlock()

	if !(reflect.ValueOf(i).Kind() == reflect.Ptr && reflect.ValueOf(i).Elem().Kind() == reflect.Struct) {
		return fmt.Errorf("passed in value must be a pointer to struct")
	}
	var (
		ty string
		ok bool
	)
	if ty, ok = d.GetType(); !ok {
		return NoNodeTypeDefinedErr
	}
	if _, err := types.FetchType(ty); err != nil {
		return err
	}

	if ty, ok = d.GetType(); !ok {
		return NoNodeTypeDefinedErr
	}
	if _, err := types.FetchType(ty); err != nil {
		return err
	}

	var (
		aty blk.TyAttrD
	)

	genAttrKey := func(attr string) string {
		if aty, ok = types.TypeC.TyAttrC[ty+":"+attr]; !ok {
			return ""
		}
		// build a item clause
		var pd strings.Builder
		//pd.WriteString(aty.P) // item partition
		pd.WriteString("A#:")
		pd.WriteString(aty.C) // attribute compressed identifier
		return pd.String()
	}

	typeOf := reflect.TypeOf(i).Elem()
	valueOf := reflect.ValueOf(i).Elem()
	for i := 0; i < typeOf.NumField(); i++ {
		field := typeOf.Field(i)
		valueField := valueOf.Field(i)
		// field name should match an attribute identifer
		attrKey := genAttrKey(field.Name)
		if attrKey == "" {
			continue
		}
		for _, v := range d.m {
			// match attribute descriptor
			if v.GetSortK() == attrKey {
				//fmt.Printf("v = %#v\n", v.SortK)
				// we now know the attribute data type, populate interface value with attribute data
				switch x := aty.DT; x {
				case "I": // int
					valueField.SetInt(v.GetI())
				case "F": // float
					valueField.SetFloat(v.GetF())
				case "S": // string
					valueField.SetString(v.GetS())
				case "Bl": // bool
					valueField.SetBool(v.GetBl())
				// case "DT": // bool
				// 	valueField.SetString(v.GetDT())
				// case "B": // binary []byte
				// 	valueField.SetBool(v.GetB())
				case "LS": // list string
					valueOf_ := reflect.ValueOf(v.GetLS())
					newSlice := reflect.MakeSlice(field.Type, 0, 0)
					valueField.Set(reflect.AppendSlice(newSlice, valueOf_))
				case "LF": // list float
					valueOf_ := reflect.ValueOf(v.GetLF())
					newSlice := reflect.MakeSlice(field.Type, 0, 0)
					valueField.Set(reflect.AppendSlice(newSlice, valueOf_))
				case "LI": // list int
					valueOf_ := reflect.ValueOf(v.GetLI())
					newSlice := reflect.MakeSlice(field.Type, 0, 0)
					valueField.Set(reflect.AppendSlice(newSlice, valueOf_))
				case "LB": // List []byte
					valueOf_ := reflect.ValueOf(v.GetLB())
					newSlice := reflect.MakeSlice(field.Type, 0, 0)
					valueField.Set(reflect.AppendSlice(newSlice, valueOf_))
				case "LBl": // List bool
					valueOf_ := reflect.ValueOf(v.GetLB())
					newSlice := reflect.MakeSlice(field.Type, 0, 0)
					valueField.Set(reflect.AppendSlice(newSlice, valueOf_))
				// case "Nd": // List []byte
				// 	valueOf_ := reflect.ValueOf(v.GetNd())
				// 	fmt.Println("In Nd: Kind(): ", valueOf_.Kind(), valueOf_.Type().Elem(), valueOf_.Len()) //  slice string 4
				// 	newSlice := reflect.MakeSlice(field.Type, 0, 0)
				// 	valueField.Set(reflect.AppendSlice(newSlice, valueOf_))
				case "IS": // set int
				case "FS": // set float
				case "SS": // set string
				case "BS": // set binary
				default:
					panic(fmt.Errorf("Unsupported data type %q", x))
				}

			}
		}
	}
	return nil

}

func (d *NodeCache) GetType() (tyN string, ok bool) {
	var di *blk.DataItem
	//syslog(fmt.Sprintf("GetType: d.m: %#v\n", d.m))
	if di, ok = d.m["A#A#T"]; !ok {
		//
		// check other predicates as most have a Ty attribute defined (currently propagated data does not)
		// this check enables us to use more specific sortK values when fetching a node rather than using top level "A#" (performance hit)
		//
		for _, di := range d.m {
			//
			if len(di.GetTy()) != 0 {
				ty, b := types.GetTyLongNm(di.GetTy())
				if b == false {
					panic(fmt.Errorf("cache.GetType() errored. Could not find long type name for short name %s", di.GetTy()))
				}
				return ty, true
			}
		}
		panic(fmt.Errorf("GetType: no A#A#T sortk entry in NodeCache"))
		return "", ok
	}
	// ty, b := types.GetTyLongNm(di.GetTy())
	// if b == false {
	// 	panic(fmt.Errorf("cache.GetType() errored. Could not find long type name for short name %s", di.GetTy()))
	// }
	//ty := di.GetTy()
	var ty string
	var b bool
	if ty, b = types.GetTyLongNm(di.GetTy()); !b {
		panic(fmt.Errorf("cache.GetType() errored. Could not find long type name for short name %s", di.GetTy()))
	}
	return ty, true

}

// PropagationTarget determines the target for scalar propagation. It is either the UID-PRED item in the parent block or an overflow
// batch item in the overflow block, determined by the number of child nodes attached and various Overflow
// system parameters. Overflow blocks are used to distribute what may be a large amount of data across a number of
// UUIDs (i.e. overflow blocks), which can then be processed in parallel if necessary without causing serious contention.
//  This routine will create the database transaction DML meta data to create the Overflow blocks (UIDs) and Overflow Batch items.
// Note: Adding Child UID mutation is not processed here to keep isolated from txh transaction. See client.AttachNode()
func (pn *NodeCache) PropagationTarget(txh *tx.Handle, cpy *blk.ChPayload, sortK string, pUID, cUID util.UID) {
	var (
		ok       bool
		err      error
		embedded int // embedded cUIDs in <parent-UID-Pred>.Nd
		oBlocks  int // overflow blocks
		//
		di *blk.DataItem // existing item
		//
		oUID  util.UID // new overflow block UID
		index int      // index in parent UID-PRED attribute Nd
		batch int64    // overflow batch id
	)
	// generates the Sortk for an overflow batch item based on the batch id and original sortK
	batchSortk := func(id int64) string {
		var s strings.Builder
		s.WriteString(sortK)
		s.WriteByte('%')
		s.WriteString(strconv.FormatInt(id, 10))
		return s.String()
	}
	// crOBatch - creates a new overflow batch and initial item to establish List/Array data
	crOBatch := func(index int) string { // return batch sortk
		//
		di.Id[index] += 1
		id := di.Id[index]
		nilItem := []byte{'0'}
		nilUID := make([][]byte, 1, 1)
		nilUID[0] = nilItem
		// xf
		xf := make([]int64, 1, 1)
		xf[0] = blk.UIDdetached // this is a nil (dummy) entry so mark it deleted.
		// entry 2: Nill batch entry - required for Dynamodb to establish List attributes
		s := batchSortk(id)
		syslog(fmt.Sprintf("PropagationTarget: create Overflow Batch - sortk %s index %d", s, index))
		upd := mut.NewMutation(tbl.EOP, oUID, s, mut.Insert)
		upd.AddMember("Nd", nilUID)
		upd.AddMember("XF", xf)
		txh.Add(upd)
		// update batch Id in parent UID
		//di.Id[index] += 1
		//r := mut.IdSet{Value: di.Id}
		//upd = mut.NewMutation(tbl.EOP, pUID, sortK, r)
		upd = mut.NewMutation(tbl.EOP, pUID, sortK, mut.Update)
		upd.AddMember("Id", di.Id, mut.Set)
		txh.Add(upd)

		return s
	}
	// crOBlock - create a new Overflow block
	crOBlock := func() string {
		// create an Overflow block UID
		oUID, err = util.MakeUID()
		if err != nil {
			panic(err)
		}
		syslog(fmt.Sprintf("PropagationTarget: create Overflow Block %v\n", oUID))
		// entry 1: P entry, containing the parent UID - to which overflow block is associated.
		// ins := mut.NewMutation(tbl.Block, oUID, "", mut.Insert)
		// ins.AddMember("P", di.GetPkey())
		// txh.Add(ins)
		txh.Add(txh.NewInsert(tbl.Block).AddMember("PKey", oUID).AddMember("Graph", types.GraphSN()).AddMember("P", di.GetPkey()))
		// add oblock to parent UID-PRED, Nd
		upd := mut.NewMutation(tbl.EOP, pUID, sortK, mut.Append) // update performs append operation based on attribute names
		o := make([][]byte, 1, 1)
		o[0] = oUID
		x := make([]int64, 1, 1)
		x[0] = blk.OvflBlockUID
		i := make([]int64, 1, 1)
		i[0] = 0
		upd.AddMember("Nd", o)
		upd.AddMember("XF", x)
		upd.AddMember("Id", i)
		txh.Add(upd)
		// update UID-PRED cache with new Overflow Block
		di.Nd = append(di.Nd, oUID)
		di.XF = append(di.XF, blk.OvflBlockUID)
		di.Id = append(di.Id, 0) // crObatch will increment to 1
		//
		// entry 2 : create batch that contains the edges in []Nd,[]XF attributes
		//
		return crOBatch(len(di.Id) - 1)
	}

	clearXF := func() {
		var updXF bool
		for i, v := range di.XF[:len(di.XF)] {
			// batch may be set to blk.OBatchSizeLimit (set when cUID is added in client.AttachNode())
			if v == blk.OBatchSizeLimit {
				// keep adding oBatches until OBatchSizeLimit reached
				di.XF[i] = blk.OvflBlockUID
				updXF = true
			}
		}
		if updXF {
			// s := mut.XFSet{Value: di.XF}
			// upd := mut.NewMutation(tbl.EOP, pUID, sortK, s)
			upd := mut.NewMutation(tbl.EOP, pUID, sortK, mut.Update)
			upd.AddMember("XF", di.XF, mut.Set)
			txh.Add(upd)
		}

	}
	syslog(fmt.Sprintf("PropagationTarget:  pUID,cUID,sortK : %s   %s   %s", pUID.String(), cUID.String(), sortK))
	//
	// get uid-pred data item from cache
	//
	if di, ok = pn.m[sortK]; !ok {
		// no uid-pred exists - create an empty one
		syslog(fmt.Sprintf("PropagationTarget: sortK not cached so create empty blk.DataItem for pUID %s", pUID))
		panic(errors.New(fmt.Sprintf("PropagationTarget: sortK not cached so create empty blk.DataItem for pUID %s", pUID)))
	}
	cpy.DI = di
	// count XF values
	for _, v := range di.XF {
		switch {
		case v <= blk.UIDdetached:
			// child  UIDs stored in parent UID-Predicate
			embedded++
		case v == blk.OBatchSizeLimit || v == blk.OvflBlockUID:
			// child UIDs stored in node overflow blocks
			oBlocks++
		}
	}
	//
	if embedded < param.EmbeddedChildNodes {
		// append  cUID  to Nd, XF to cached  di (not necessary or is it ???).
		// Database meta-data update in calling client.AttachNode().
		di.Nd = append(di.Nd, cUID)
		di.XF = append(di.XF, blk.OvflBlockUID)
		di.Id = append(di.Id, 0)
		// child node attachment point is the parent UID
		cpy.TUID = pUID

		return
	}
	//
	// overflow blocks required....
	//
	if oBlocks <= param.MaxOvFlBlocks {

		// create oBlocks from 1..param.MaxOvFlBlocks with upto param.OBatchThreshold batches in each
		// only interested in last entry in Nd, Id UID-pred arrays, as that is the one GoGraph
		// is initially filling up until OBatchThreshold batches.
		index = len(di.Nd) - 1
		oUID = di.Nd[index]
		batch = di.Id[index]
		//
		switch {

		case oBlocks == 0:

			s := crOBlock()

			cpy.TUID = oUID
			cpy.BatchId = 1
			cpy.Osortk = s
			cpy.NdIndex = len(di.Nd) - 1

		case di.XF[index] == blk.OBatchSizeLimit && batch < param.OBatchThreshold:

			clearXF()
			//fmt.Printf("PropagationTarget;  1  oBlocks %d   batch %d\n", oBlocks, batch)
			crOBatch(index)
			//fmt.Printf("PropagationTarget; 1a  oBlocks %d   batch %d\n", oBlocks, di.Id[index])
			batch = di.Id[index]
			cpy.TUID = oUID
			cpy.BatchId = batch // batch
			cpy.Osortk = batchSortk(batch)
			cpy.NdIndex = index

		case di.XF[index] == blk.OBatchSizeLimit && batch == param.OBatchThreshold:

			//fmt.Printf("PropagationTarget;  2  oBlocks %d   batch %d\n", oBlocks, batch)

			if oBlocks < param.MaxOvFlBlocks {

				// reached OBatchThreshold batches in current OBlock.
				// Add another oBlock - ultimately MaxOvFlBlocks will be reacehd.
				clearXF()
				s := crOBlock()
				//fmt.Printf("PropagationTarget; 2a  oBlocks %d   batch %d\n", oBlocks+1, di.Id[index])
				batch = di.Id[len(di.Id)-1]
				cpy.TUID = oUID
				cpy.BatchId = batch // 1
				cpy.Osortk = s      //batchSortk(batch)
				cpy.NdIndex = len(di.Id) - 1

			} else {

				cpy.Random = true
				//fmt.Printf("PropagationTarget; 2b  oBlocks %d   batch %d.  break \n", oBlocks+1, di.Id[index])
				//rand.Seed(time.Now().UnixNano())
				//TODO: try rand.Int64n
				index = rand.Intn(len(di.Nd)-param.EmbeddedChildNodes) + param.EmbeddedChildNodes
				cpy.TUID = di.Nd[index]
				//rand.Seed(time.Now().UnixNano())
				bid := rand.Intn(int(di.Id[index])) + 1
				//fmt.Printf("Randomly chosen index: %d bid  %d\n", index, bid)
				cpy.BatchId = int64(bid)
				cpy.Osortk = batchSortk(int64(bid))
				cpy.NdIndex = index
			}

		case di.XF[index] != blk.OBatchSizeLimit && batch <= param.OBatchThreshold:

			//fmt.Printf("PropagationTarget;  3  oBlocks %d   batch %d\n", oBlocks, di.Id[index])

			batch = di.Id[index]
			cpy.TUID = oUID
			cpy.BatchId = batch // batch
			cpy.Osortk = batchSortk(batch)
			cpy.NdIndex = index

		default:
			panic("switch in PropagationTarget did not process any case options...")
		}

	}

}

// type FacetAttr struct {
// 	Attr  string
// 	Facet string
// 	Ty  string
// 	Abrev string
// }
// type expression struct {
// 	arg []Arguments
// 	expr
// }

// type Attribute struct {
// 	alias  string
// 	name   string
// 	args   []Arguments
// 	facets []Facet
// 	filter []Filter
// 	attrs  []attribute
// }

// func (u *UIDs) Attr() {}

// type query struct {
// 	alias
// 	name    string
// 	var_    string
// 	f       string
// 	cascade bool
// 	filter  []Filter
// 	attr    []attribute
// 	args    []Arguments
// }

type FacetTy struct {
	Name string
	DT   string
	C    string
}

type FacetCache map[types.TyAttr][]FacetTy

var FacetC FacetCache

func AddReverseEdge(cuid, puid []byte, pTy string, sortk string) error {
	return nil
}
