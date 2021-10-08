package spannerdriver

var jsonFile = `{
  "statements":
  [
    {
      "name": "SHOW VARIABLE RETRY_ABORTS_INTERNALLY",
      "executorName": "ClientSideStatementNoParamExecutor",
      "resultType": "RESULT_SET",
      "regex": "(?is)\\A\\s*show\\s+variable\\s+retry_aborts_internally\\s*\\z",
      "method": "statementShowRetryAbortsInternally",
      "exampleStatements": ["show variable retry_aborts_internally"],
      "examplePrerequisiteStatements": ["set readonly=false", "set autocommit=false"]
    },
    {
      "name": "SHOW VARIABLE AUTOCOMMIT_DML_MODE",
      "executorName": "ClientSideStatementNoParamExecutor",
      "resultType": "RESULT_SET",
      "regex": "(?is)\\A\\s*show\\s+variable\\s+autocommit_dml_mode\\s*\\z",
      "method": "statementShowAutocommitDmlMode",
      "exampleStatements": ["show variable autocommit_dml_mode"]
    },
    {
      "name": "START BATCH DDL",
      "executorName": "ClientSideStatementNoParamExecutor",
      "resultType": "NO_RESULT",
      "regex": "(?is)\\A\\s*(?:start)(?:\\s+batch)(?:\\s+ddl)\\s*\\z",
      "method": "statementStartBatchDdl",
      "exampleStatements": ["start batch ddl"]
    },
    {
      "name": "START BATCH DML",
      "executorName": "ClientSideStatementNoParamExecutor",
      "resultType": "NO_RESULT",
      "regex": "(?is)\\A\\s*(?:start)(?:\\s+batch)(?:\\s+dml)\\s*\\z",
      "method": "statementStartBatchDml",
      "exampleStatements": ["start batch dml"]
    },
    {
      "name": "RUN BATCH",
      "executorName": "ClientSideStatementNoParamExecutor",
      "resultType": "NO_RESULT",
      "regex": "(?is)\\A\\s*(?:run)(?:\\s+batch)\\s*\\z",
      "method": "statementRunBatch",
      "exampleStatements": ["run batch"],
      "examplePrerequisiteStatements": ["start batch ddl"]
    },
    {
      "name": "ABORT BATCH",
      "executorName": "ClientSideStatementNoParamExecutor",
      "resultType": "NO_RESULT",
      "regex": "(?is)\\A\\s*(?:abort)(?:\\s+batch)\\s*\\z",
      "method": "statementAbortBatch",
      "exampleStatements": ["abort batch"],
      "examplePrerequisiteStatements": ["start batch ddl"]
    },
    {
      "name": "SET RETRY_ABORTS_INTERNALLY = TRUE|FALSE",
      "executorName": "ClientSideStatementSetExecutor",
      "resultType": "NO_RESULT",
      "regex": "(?is)\\A\\s*set\\s+retry_aborts_internally\\s*(?:=)\\s*(.*)\\z",
      "method": "statementSetRetryAbortsInternally",
      "exampleStatements": ["set retry_aborts_internally = true", "set retry_aborts_internally = false"],
      "examplePrerequisiteStatements": ["set readonly = false", "set autocommit = false"],
      "setStatement": {
        "propertyName": "RETRY_ABORTS_INTERNALLY",
        "separator": "=",
        "allowedValues": "(TRUE|FALSE)",
        "converterName": "ClientSideStatementValueConverters$BooleanConverter"
      }
    },
    {
      "name": "SET AUTOCOMMIT_DML_MODE = 'PARTITIONED_NON_ATOMIC'|'TRANSACTIONAL'",
      "executorName": "ClientSideStatementSetExecutor",
      "resultType": "NO_RESULT",
      "regex": "(?is)\\A\\s*set\\s+autocommit_dml_mode\\s*(?:=)\\s*(.*)\\z",
      "method": "statementSetAutocommitDmlMode",
      "exampleStatements": ["set autocommit_dml_mode='PARTITIONED_NON_ATOMIC'", "set autocommit_dml_mode='TRANSACTIONAL'"],
      "setStatement": {
        "propertyName": "AUTOCOMMIT_DML_MODE",
        "separator": "=",
        "allowedValues": "'(PARTITIONED_NON_ATOMIC|TRANSACTIONAL)'",
        "converterName": "ClientSideStatementValueConverters$AutocommitDmlModeConverter"
      }
    }
  ]
}
`
