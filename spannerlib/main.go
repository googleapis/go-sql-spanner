package main

import "C"
import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"spannerlib/exported"
)

// An (empty) main function is required for C libraries.
func main() {}

var (
	pinners   = sync.Map{}
	pinnerIdx atomic.Int64
)

// Release releases (unpins) a previously pinned message.
//
//export Release
func Release(ptr int64) int32 {
	val, ok := pinners.LoadAndDelete(ptr)
	if !ok {
		return 1
	}
	pinner := val.(*runtime.Pinner)
	pinner.Unpin()
	return 0
}

func pin(msg *exported.Message) (int64, int32, int64, int32, unsafe.Pointer) {
	pinner := &runtime.Pinner{}
	if msg.Res != nil {
		pinner.Pin(&(msg.Res[0]))
	}
	idx := pinnerIdx.Add(1)
	pinners.Store(idx, pinner)
	return idx, msg.Code, msg.ObjectId, msg.Length(), msg.ResPointer()
}

//export CreatePool
func CreatePool() (int64, int32, int64, int32, unsafe.Pointer) {
	msg := exported.CreatePool()
	return pin(msg)
}

//export ClosePool
func ClosePool(id int64) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := exported.ClosePool(id)
	return pin(msg)
}

//export CreateConnection
func CreateConnection(poolId int64, project, instance, database string) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := exported.CreateConnection(poolId, project, instance, database)
	return pin(msg)
}

//export CloseConnection
func CloseConnection(poolId, connId int64) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := exported.CloseConnection(poolId, connId)
	return pin(msg)
}

//export Execute
func Execute(poolId, connectionId int64, statement []byte) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := exported.Execute(poolId, connectionId, statement)
	return pin(msg)
}

//export Next
func Next(poolId, connId, rowsId int64) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := exported.Next(poolId, connId, rowsId)
	return pin(msg)
}

//export CloseRows
func CloseRows(poolId, connId, rowsId int64) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := exported.CloseRows(poolId, connId, rowsId)
	return pin(msg)
}
