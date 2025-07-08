package main

import "C"
import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"spannerlib/lib"
)

// An (empty) main function is required for C libraries.
func main() {}

var (
	pinners   = sync.Map{}
	pinnerIdx atomic.Int64
)

// Release releases (unpins) a previously pinned message. Pinners are created and
// returned for each function call in this library, and it is the responsibility of
// the caller to call Release when it is done with the data that was returned.
// Note that 'done' can also mean 'the data has been copied into memory that is
// managed by the caller'.
//
//export Release
func Release(pinnerId int64) int32 {
	if pinnerId <= 0 {
		return 0
	}
	val, ok := pinners.LoadAndDelete(pinnerId)
	if !ok {
		return 1
	}
	pinner := val.(*runtime.Pinner)
	pinner.Unpin()
	return 0
}

// pin pins the memory location pointed to by the result of the given message.
// This prevents the Go runtime from moving or garbage collecting this memory.
// The returned pinner ID must be used to call Release when the caller is done
// with the message.
func pin(msg *lib.Message) (int64, int32, int64, int32, unsafe.Pointer) {
	if msg.Length() == 0 {
		return 0, msg.Code, msg.ObjectId, 0, nil
	}
	pinner := &runtime.Pinner{}
	pinner.Pin(&(msg.Res[0]))
	idx := pinnerIdx.Add(1)
	pinners.Store(idx, pinner)
	return idx, msg.Code, msg.ObjectId, msg.Length(), msg.ResPointer()
}

// CreatePool creates a pool of database connections. A Pool is equivalent to a *sql.DB.
//
//export CreatePool
func CreatePool(dsn string) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := lib.CreatePool(dsn)
	return pin(msg)
}

// ClosePool closes a previously opened Pool.
//
//export ClosePool
func ClosePool(id int64) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := lib.ClosePool(id)
	return pin(msg)
}

// CreateConnection creates or borrows a connection from a previously created pool.
// Note that as Spanner does not really use a 'connection-based' API, creating a
// connection is a relatively cheap operation. It does not physically create a new
// gRPC channel or any other physical connection to Spanner, and it also does not
// create a server-side session. Instead, all session state is stored in the client.
//
//export CreateConnection
func CreateConnection(poolId int64) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := lib.CreateConnection(poolId)
	return pin(msg)
}

//export CloseConnection
func CloseConnection(poolId, connId int64) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := lib.CloseConnection(poolId, connId)
	return pin(msg)
}

// Apply applies an array of mutations in a single read/write transaction.
// The mutationsBytes must be an encoded BatchWriteRequest_MutationGroup protobuf object.
//
//export Apply
func Apply(poolId, connectionId int64, mutationsBytes []byte) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := lib.Apply(poolId, connectionId, mutationsBytes)
	return pin(msg)
}

// BufferWrite buffers an array of mutations in the given read/write transaction.
// The mutationsBytes must be an encoded BatchWriteRequest_MutationGroup protobuf object.
//
//export BufferWrite
func BufferWrite(poolId, connectionId, txId int64, mutationsBytes []byte) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := lib.BufferWrite(poolId, connectionId, txId, mutationsBytes)
	return pin(msg)
}

// Execute executes a SQL statement on the given connection.
// The return type is an identifier for a Rows object. This identifier can be used to
// call the functions Metadata and Next to get respectively the metadata of the result
// and the next row of results.
//
//export Execute
func Execute(poolId, connectionId int64, statement []byte) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := lib.Execute(poolId, connectionId, statement)
	return pin(msg)
}

// ExecuteTransaction executes a statement using a specific transaction.
//
//export ExecuteTransaction
func ExecuteTransaction(poolId, connectionId, txId int64, statement []byte) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := lib.ExecuteTransaction(poolId, connectionId, txId, statement)
	return pin(msg)
}

// ExecuteBatchDml executes a batch of DML statements on the given connection.
//
//export ExecuteBatchDml
func ExecuteBatchDml(poolId, connectionId int64, statements []byte) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := lib.ExecuteBatchDml(poolId, connectionId, statements)
	return pin(msg)
}

// Metadata returns the metadata of a Rows object.
//
//export Metadata
func Metadata(poolId, connId, rowsId int64) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := lib.Metadata(poolId, connId, rowsId)
	return pin(msg)
}

// ResultSetStats returns the statistics for a statement that has been executed. This includes
// the number of rows affected in case of a DML statement.
// Statistics are only available once all rows have been consumed.
//
//export ResultSetStats
func ResultSetStats(poolId, connId, rowsId int64) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := lib.ResultSetStats(poolId, connId, rowsId)
	return pin(msg)
}

// Next returns the next row in a Rows object. The returned message contains a protobuf
// ListValue that contains all the columns of the row. The message is empty if there are
// no more rows in the Rows object.
//
//export Next
func Next(poolId, connId, rowsId int64) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := lib.Next(poolId, connId, rowsId)
	return pin(msg)
}

// CloseRows closes and cleans up all memory held by a Rows object. This must be called
// when the application is done with the Rows object.
//
//export CloseRows
func CloseRows(poolId, connId, rowsId int64) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := lib.CloseRows(poolId, connId, rowsId)
	return pin(msg)
}

// BeginTransaction begins a new transaction on the given connection.
// The txOpts byte slice contains a serialized protobuf TransactionOptions object.
//
//export BeginTransaction
func BeginTransaction(poolId, connectionId int64, txOpts []byte) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := lib.BeginTransaction(poolId, connectionId, txOpts)
	return pin(msg)
}

// Commit commits a previously started transaction. All transactions must be either
// committed or rolled back, including read-only transactions. This to ensure that
// all resources that are held by a transaction are cleaned up.
//
//export Commit
func Commit(poolId, connectionId, txId int64) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := lib.Commit(poolId, connectionId, txId)
	return pin(msg)
}

// Rollback rolls back a previously started transaction. All transactions must be either
// committed or rolled back, including read-only transactions. This to ensure that
// all resources that are held by a transaction are cleaned up.
//
// Spanner does not require read-only transactions to be committed or rolled back, but
// this library requires that all transactions are committed or rolled back to clean up
// all resources. Commit and Rollback are semantically the same for read-only transactions.
//
//export Rollback
func Rollback(poolId, connectionId, txId int64) (int64, int32, int64, int32, unsafe.Pointer) {
	msg := lib.Rollback(poolId, connectionId, txId)
	return pin(msg)
}
