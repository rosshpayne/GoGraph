package tbl

type Name string

const (
	Block      Name = "Block"
	Edge       Name = "Edge"
	NodeScalar Name = "NodeScalar"
	Propagated Name = "PropagatedScalar"
	Type       Name = "GoGraphSS"
	Event      Name = "EventLog"
	//AttachDetachEvent Name = "NodeAttachDetachEvent"
)

type key struct {
	Pk string
	Sk string
}

type KeyMap map[Name]key

var (
	err  error
	Keys KeyMap
)

func init() {

	Keys = KeyMap{
		Block:      key{Pk: "PKey"},
		Edge:       key{"PKey", "SortK"},
		NodeScalar: key{"PKey", "SortK"},
		Propagated: key{"PKey", "SortK"},
		Event:      key{Pk: "eid"},
		//AttachDetachEvent: key{Pk: "eid"},
	}

}

func Register(t Name, pk string, sk ...string) {
	var k key
	if len(sk) > 0 {
		k = key{pk, sk[0]} // must be of type util.UID
	} else {
		k = key{Pk: pk}
	}
	Keys[t] = k
}

// func New() {

// func Register (){}
