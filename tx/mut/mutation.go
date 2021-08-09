package tx

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/GoGraph/dbConn"
	"github.com/GoGraph/util"
	//"google.golang.org/api/spanner/v1"

	"cloud.google.com/go/spanner" //v1.21.0
	"google.golang.org/grpc/codes"
)

type StdDML byte

const (
	PropagateMerger StdDML = 'P'
	Merge           StdDML = 'M'
	Insert          StdDML = 'I'
	Update          StdDML = 'U' // update performing "set =" operation
	//Delete 		StdDML = 'D'
	Append StdDML = 'A' // update performing array/list append operation on attributes
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
	client spanner.Client
)

func init() {

	client = dbConn.New()
}

//
// database API meta structures
//
type Member struct {
	//sortk string
	Name  string
	Param string
	Value interface{}
	Opr   StdDML // for update of numerics. Add rather than set e.g. set col = col + @v1. Default: set col=@v1
}

type condition struct {
	f   string // func e.g. contains
	arg []Member
	mod string
}

type Mutation struct {
	ms  []Member
	cd  condition
	pk  util.UIDs
	sk  string
	tbl string
	opr interface{}
}

type Mutations []Mutation

func (im Mutations) Add(mut Mutation) Mutations {
	im = append(im, mut)
	return im
}

func NewMutation(table string, pk util.UID, sk string, opr interface{}) Mutation {
	return Mutation{tbl: table, pk: pk, sk: sk, opr: opr}
}

// func NewMutationEventLog(table string, pk  opr interface{}) *Mutation {
// 	return &Mutation{tbl: table, pk: pk, sk: sk, opr: opr}
// }

func (m *Mutations) SetOpr(opr interface{}) {
	m.opr = opr
}
func (m *Mutation) GetMembers() []Member {
	return m.ms
}

func (m *Mutation) GetOpr() interface{} {
	return m.opr
}

func (m *Mutation) GetTable() string {
	return m.tbl
}

func (im *Mutation) AddMember(attr string, value interface{}, opr ...StdDML) { //TODO: is opr necessary

	p := strings.Replace(attr, "#", "_")
	p = strings.Replace(p, ":", "x")
	if p[0] == '0' {
		p = '1' + p
	}
	if len(opr) > 0 {
		im.Ms = append(im.Ms, member{Name: attr, Param: "@" + p, Value: value, Opr: opr})
	} else {
		im.Ms = append(im.Ms, member{Name: attr, Param: "@" + p, Value: value, Opr: "Set"})
	}
}
func (ip *Mutate) AddMember2(attr string, p string, value interface{}, opr ...byte) {

	if len(opr) > 0 {
		ip.ms = append(ip.ms, Member{item: attr, param: "@" + p, value: value, opr: opr})
	} else {
		ip.ms = append(ip.ms, Member{item: attr, param: "@" + p, value: value, opr: "Set"})
	}
}

func (ip *Mutate) AddCondition(m string, value interface{}, mod ...string) {}
