// Copyright 2021 Google LLC All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spannerdriver

var jsonFile = `{
  "statements":
  [
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
    }
  ]
}
`
