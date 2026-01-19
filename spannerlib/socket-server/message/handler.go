package message

type Handler interface {
	HandleStartup(msg *StartupMessage) error
	HandleExecute(msg *ExecuteMessage) error
	HandleExecuteBatch(msg *ExecuteBatchMessage) error
	HandleRows(msg *RowsMessage) error
	HandleBatchResult(msg *BatchResultMessage) error

	HandleBegin(msg *BeginMessage) error
	HandleCommit(msg *CommitMessage) error
	HandleRollback(msg *RollbackMessage) error
	HandleCommitResult(msg *CommitResultMessage) error

	HandleStatus(msg *StatusMessage) error
}
