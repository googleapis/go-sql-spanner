package exported

import "database/sql"

type transaction struct {
	backend *sql.Tx
}

func Commit(poolId, connId, txId int64) *Message {
	res, err := findTx(poolId, connId, txId)
	if err != nil {
		return errMessage(err)
	}
	return res.Metadata()
}
