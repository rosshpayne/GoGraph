package db

import (
	"github.com/GoGraph/block"
	"github.com/GoGraph/dbs"
	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/util"
)

type WithOBatchLimit struct {
	Ouid   util.UID
	Cuid   util.UID
	Puid   util.UID
	DI     *block.DataItem
	OSortK string // overflow sortk
	Index  int    // UID-PRED Nd index entry
}

func (x *WithOBatchLimit) GetStatements() []dbs.Statement {

	cuid := make([][]byte, 1)
	cuid[0] = x.Cuid
	xf := make([]int64, 1)
	xf[0] = int64(block.ChildUID)

	upd1 := dbs.Statement{
		SQL: "update EOP set Nd=ARRAY_CONCAT(Nd,@cuid), XF=ARRAY_CONCAT(XF,@status) where PKey=@pk and SortK = @sk",
		Params: map[string]interface{}{
			"pk":     x.Ouid,
			"sk":     x.OSortK,
			"cuid":   cuid,
			"status": xf,
		},
	}
	// set OBatchSizeLimit if array size reaches param.OvflBatchSize
	xf = x.DI.XF // or GetXF()
	xf[x.Index] = block.OBatchSizeLimit
	upd2 := dbs.Statement{
		SQL: "update EOP x set XF=@xf where PKey=@pk and Sortk = @sk and @size = (select ARRAY_LENGTH(XF)-1 from EOP  where SortK  = @osk and PKey = @opk)",
		Params: map[string]interface{}{
			"pk":   x.DI.Pkey,
			"sk":   x.DI.GetSortK(),
			"xf":   xf,
			"opk":  x.Ouid,
			"osk":  x.OSortK,
			"size": param.OvfwBatchSize,
		},
	}

	return []dbs.Statement{upd1, upd2}
}
