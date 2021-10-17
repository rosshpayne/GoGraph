package ds

import (
	"github.com/GoGraph/util"
)

type Edge struct {
	Puid, Cuid util.UID
	Sortk      string
	RespCh     chan bool
}

func NewEdge() *Edge {
	return &Edge{}
}
