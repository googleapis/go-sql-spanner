package spannerdriver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

var reMissingDefaultSequenceKind = regexp.MustCompile(`Please specify the sequence kind explicitly or set the database option\s+['\x60]?default_sequence_kind['\x60]?\.`)

func isMissingDefaultSequenceKindError(err error) bool {
	if err == nil {
		return false
	}
	return reMissingDefaultSequenceKind.MatchString(err.Error())
}

func (c *conn) executeDDLWithDefaultSequenceKindRetry(ctx context.Context, originalStatements []spanner.Statement, ddlStatements []string) (driver.Result, error) {
	op, err := c.adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   c.database,
		Statements: ddlStatements,
	})

	var opRetry *adminapi.UpdateDatabaseDdlOperation
	var restartIndex int
	var retryErr error

	if err != nil {
		// The RPC execution returned an error.
		defaultSequenceKind := propertyDefaultSequenceKind.GetValueOrDefault(c.state)
		if defaultSequenceKind != "" && isMissingDefaultSequenceKindError(err) {
			if errAlter := c.setDefaultSequenceKind(ctx, defaultSequenceKind); errAlter == nil {
				opRetry, retryErr = c.adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
					Database:   c.database,
					Statements: ddlStatements,
				})
			}
		}
	} else {
		c.lastDDLOperationID = op.Name()
		err = c.waitForDDLOperation(ctx, op.Name(), func(ctx context.Context) error {
			return op.Wait(ctx)
		})
		if err != nil {
			// The long-running operation returned an error.
			defaultSequenceKind := propertyDefaultSequenceKind.GetValueOrDefault(c.state)
			if defaultSequenceKind != "" && isMissingDefaultSequenceKindError(err) {
				if errAlter := c.setDefaultSequenceKind(ctx, defaultSequenceKind); errAlter == nil {
					restartIndex = getSuccessCount(op)
					if restartIndex < len(ddlStatements) {
						opRetry, retryErr = c.adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
							Database:   c.database,
							Statements: ddlStatements[restartIndex:],
						})
					}
				}
			}
		}
	}

	// If a retry was successfully scheduled
	if opRetry != nil && retryErr == nil {
		c.lastDDLOperationID = opRetry.Name()
		err = c.waitForDDLOperation(ctx, opRetry.Name(), func(ctx context.Context) error {
			return opRetry.Wait(ctx)
		})
		if err == nil {
			mode := propertyDDLExecutionMode.GetValueOrDefault(c.state)
			if mode == DDLExecutionModeAsync || mode == DDLExecutionModeAsyncWait {
				return &result{operationID: opRetry.Name()}, nil
			}
			return driver.ResultNoRows, nil
		}
	} else if retryErr != nil {
		err = retryErr
	}

	if err != nil {
		if len(originalStatements) > 1 {
			be := &BatchError{
				Err:               err,
				BatchUpdateCounts: []int64{},
			}
			successCount := restartIndex
			if opRetry != nil {
				successCount += getSuccessCount(opRetry)
			}
			for i := 0; i < successCount; i++ {
				be.BatchUpdateCounts = append(be.BatchUpdateCounts, int64(-1))
			}
			return nil, be
		}
		return nil, err
	}

	mode := propertyDDLExecutionMode.GetValueOrDefault(c.state)
	if mode == DDLExecutionModeAsync || mode == DDLExecutionModeAsyncWait {
		return &result{operationID: op.Name()}, nil
	}
	return driver.ResultNoRows, nil
}

func (c *conn) setDefaultSequenceKind(ctx context.Context, defaultSequenceKind string) error {
	dbID := c.databaseID()
	var alterStatement string
	if c.parser.Dialect == adminpb.DatabaseDialect_POSTGRESQL {
		alterStatement = fmt.Sprintf(`ALTER DATABASE "%s" SET spanner.default_sequence_kind = '%s'`, strings.ReplaceAll(dbID, `"`, `""`), defaultSequenceKind)
	} else {
		alterStatement = fmt.Sprintf("ALTER DATABASE `%s` SET OPTIONS (default_sequence_kind = '%s')", strings.ReplaceAll(dbID, "`", "``"), defaultSequenceKind)
	}
	opAlter, errAlter := c.adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   c.database,
		Statements: []string{alterStatement},
	})
	if errAlter != nil {
		return errAlter
	}
	return c.waitForDDLOperation(ctx, opAlter.Name(), func(ctx context.Context) error {
		return opAlter.Wait(ctx)
	})
}

func (c *conn) databaseID() string {
	parts := strings.Split(c.database, "/")
	return parts[len(parts)-1]
}

func getSuccessCount(op *adminapi.UpdateDatabaseDdlOperation) int {
	if op == nil {
		return 0
	}
	metadata, err := op.Metadata()
	if err != nil || metadata == nil {
		return 0
	}
	var count int
	for _, ts := range metadata.CommitTimestamps {
		if ts != nil {
			count++
		} else {
			break
		}
	}
	return count
}
