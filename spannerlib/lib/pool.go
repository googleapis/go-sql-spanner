package lib

import "C"
import (
	"fmt"

	"spannerlib/api"
)

func CreatePool(dsn string) *Message {
	// Copy the DSN into Go managed memory, as the DSN is stored in the connector config that is created.
	dsn = fmt.Sprintf("%s", dsn)

	id, err := api.CreatePool(dsn)
	if err != nil {
		return errMessage(err)
	}
	return idMessage(id)
}

func ClosePool(id int64) *Message {
	if err := api.ClosePool(id); err != nil {
		return errMessage(err)
	}
	return &Message{}
}

func CreateConnection(poolId int64) *Message {
	conn, err := api.CreateConnection(poolId)
	if err != nil {
		return errMessage(err)
	}
	return idMessage(conn)
}
