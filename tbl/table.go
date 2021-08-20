package tbl

import (
	"fmt"
	"sync"
)

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
	pk string
	sk string
}

type KeyPass struct {
	Pk string
	Sk string
}

type keyMap map[Name]key

var keysync sync.RWMutex

var (
	err  error
	keys keyMap
)

func init() {

	keys = keyMap{
		Block:      key{pk: "PKey"},
		Edge:       key{"PKey", "SortK"},
		NodeScalar: key{"PKey", "SortK"},
		Propagated: key{"PKey", "SortK"},
		Event:      key{pk: "eid"},
		//AttachDetachEvent: key{Pk: "eid"},
	}

}

func Register(t Name, pk string, sk ...string) {
	var k key
	if len(sk) > 0 {
		k = key{pk, sk[0]} // must be of type util.UID
	} else {
		k = key{pk: pk}
	}
	keysync.Lock()
	keys[t] = k
	keysync.Unlock()
}

func GetKeys(t Name) (*KeyPass, error) {

	keysync.RLock()
	k, ok := keys[t]
	keysync.RUnlock()
	if !ok {
		return nil, fmt.Errorf("Table %s not found", t)
	}
	return &KeyPass{Pk: k.pk, Sk: k.sk}, nil
}
