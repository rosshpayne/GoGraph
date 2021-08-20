package mut

import (
	"fmt"
	"strings"

	blk "github.com/GoGraph/block"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/util"
	//"google.golang.org/api/spanner/v1"

	"cloud.google.com/go/spanner" //v1.21.0
)

type StdDML byte

const (
	PropagateMerger StdDML = 'P'
	Merge           StdDML = 'M'
	Insert          StdDML = 'I'
	Update          StdDML = 'U' // update performing "set =" operation
	//Delete 		StdDML = 'D'
	Append         StdDML = 'A' // update performing array/list append operation on attributes
	PropagateMerge StdDML = 'R'
)

// set a Id entry - not supported by Spanner Arrays so not used. Use IdSet{} instead.
// type IdIndexSet struct {
// 	Value int
// 	Index int
// }
// type XfIndexSet struct  {
// 	Value int
// 	Index int
// }
type IdSet struct {
	Value []int
}

type XFSet struct {
	Value []int
}

// add cUID to target UID (only applies to oUID's) then update pUID XF to BatchFULL if exceeded batch limit
//r:=tx.WithOBatchLimit{Ouid: oUID, Cuid: cUID, Puid: pUID, OSortK: s.String(), Index: index}
type WithOBatchLimit struct {
	Ouid   util.UID
	Cuid   util.UID
	Puid   util.UID
	DI     *blk.DataItem
	OSortK string // overflow sortk
	Index  int    // UID-PRED Nd index entry
}

var (
	err    error
	client *spanner.Client
)

//
// database API meta structures
//
type Member struct {
	//sortk string
	Name  string
	Param string
	Value interface{}
	//Opr   StdDML // for update of numerics. Add rather than set e.g. set col = col + @v1. Default: set col=@v1
}

type condition struct {
	f   string // func e.g. contains
	arg []Member
	mod string
}

type Mutation struct {
	ms  []Member
	cd  condition
	pk  util.UID
	sk  string
	tbl tbl.Name
	opr interface{}
}

type Mutations []*Mutation

func (im *Mutations) Add(mut *Mutation) {
	*im = append(*im, mut)
}

func (ms *Mutations) Reset() {
	*ms = nil
}

func NewMutation(table tbl.Name, pk util.UID, sk string, opr interface{}) *Mutation {

	keys, ok := tbl.Keys[table]
	if !ok {
		panic(fmt.Errorf("Table %q is unknown", table))
	}

	mut := &Mutation{tbl: table, pk: pk, sk: sk, opr: opr}

	// presumes all Primary Keys are a UUID
	mut.AddMember(keys.Pk, []byte(pk))
	if len(keys.Sk) != 0 {
		mut.AddMember(keys.Sk, sk)
	}

	return mut
}

// func NewMutationEventLog(table string, pk  opr interface{}) *Mutation {
// 	return &Mutation{tbl: table, pk: pk, sk: sk, opr: opr}
// }

func (m *Mutation) SetOpr(opr interface{}) {
	m.opr = opr
}

func (m *Mutation) GetMembers() []Member {
	return m.ms
}

func (m *Mutation) GetOpr() interface{} {
	return m.opr
}

func (m *Mutation) GetPK() util.UID {
	return m.pk
}

func (m *Mutation) GetSK() string {
	return m.sk
}

func (m *Mutation) GetTable() string {
	return string(m.tbl)
}

func (im *Mutation) AddMember(attr string, value interface{}) *Mutation { //, opr ...StdDML) { //TODO: is opr necessary

	p := strings.Replace(attr, "#", "_", -1)
	p = strings.Replace(p, ":", "x", -1)
	if p[0] == '0' {
		p = "1" + p
	}
	fmt.Println("AddMember: ", attr, p, value)
	// change param names for PKey & SortK
	// switch attr {
	// case "PKey":
	// 	p = "pk"
	// case "SortK":
	// 	p = "sk"
	// }
	m := Member{Name: attr, Param: "@" + p, Value: value}
	im.ms = append(im.ms, m)

	return im
}

// func (ip *Mutation) AddMember2(attr string, p string, value interface{}, opr ...byte) {

// 	if len(opr) > 0 {
// 		ip.ms = append(ip.ms, Member{item: attr, param: "@" + p, value: value, opr: opr})
// 	} else {
// 		ip.ms = append(ip.ms, Member{item: attr, param: "@" + p, value: value, opr: "Set"})
// 	}
// }

func (ip *Mutation) AddCondition(m string, value interface{}, mod ...string) {}
