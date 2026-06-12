// Copyright 2026 Google LLC
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

// Suppress no-explicit-any for decoding raw dynamic row structs returned by Spanner
/* eslint-disable @typescript-eslint/no-explicit-any */
import * as assert from 'assert';
import { Pool } from '../../src/lib/pool.js';
import {
  MockSpanner,
  StatementResult,
  createSelect1ResultSet,
  createResultSetWithAllDataTypes,
} from './mockspanner.js';

describe('End-to-End Execution on MockServer', () => {
  let mock: MockSpanner;
  let port: number;
  let pool: Pool;

  before(async () => {
    mock = MockSpanner.create();
    port = await mock.listen('127.0.0.1:0');
    const connStr = `localhost:${port}/projects/p/instances/i/databases/d?useplaintext=true`;
    pool = await Pool.create(connStr);
  });

  after(async () => {
    if (pool) {
      await pool.close();
    }
    mock?.close();
  });

  it('should execute SQL end-to-end and Row fetching', async () => {
    mock.putStatementResult(
      'SELECT 1',
      StatementResult.resultSet(createSelect1ResultSet())
    );

    const connection = await pool.createConnection();
    assert.ok(connection, 'Connection created');

    const rows = await connection.executeSql('SELECT 1');
    assert.ok(rows, 'Rows returned');

    const row: any = await rows.next();
    assert.ok(row, 'Row decoded successfully');
    assert.strictEqual(
      row.values[0].stringValue,
      '1',
      'Decoded value matches result'
    );
    assert.strictEqual(await rows.next(), null, 'Iterator complete');

    await rows.close();
    await connection.close();
  });

  it('should decode all Spanner data types correctly across the FFI boundary', async () => {
    mock.putStatementResult(
      'SELECT * FROM AllTypes',
      StatementResult.resultSet(createResultSetWithAllDataTypes())
    );

    const connection = await pool.createConnection();
    const rows = await connection.executeSql('SELECT * FROM AllTypes');
    const row: any = await rows.next();
    assert.ok(row, 'Row decoded successfully');

    assert.strictEqual(row.values[0].boolValue, true, 'BOOL decoded');
    assert.strictEqual(row.values[1].stringValue, '1', 'INT64 decoded');
    assert.strictEqual(row.values[2].numberValue, 3.14, 'FLOAT64 decoded');
    assert.strictEqual(row.values[4].stringValue, 'One', 'STRING decoded');

    await rows.close();
    await connection.close();
  });

  it('should handle concurrent query execution across multiple connections', async () => {
    mock.putStatementResult(
      'SELECT 1',
      StatementResult.resultSet(createSelect1ResultSet())
    );

    const connections = await Promise.all([
      pool.createConnection(),
      pool.createConnection(),
      pool.createConnection(),
    ]);

    assert.strictEqual(connections.length, 3, '3 concurrent connections');

    const results = await Promise.all(
      connections.map((c) => c.executeSql('SELECT 1'))
    );

    for (const rows of results) {
      const row: any = await rows.next();
      assert.strictEqual(row.values[0].stringValue, '1');
      await rows.close();
    }

    await Promise.all(connections.map((c) => c.close()));
  });

  it('should properly handle backend gRPC errors and propagate them', async () => {
    mock.putStatementResult(
      'SELECT * FROM InvalidTable',
      StatementResult.error(new Error('Table InvalidTable not found'))
    );

    const connection = await pool.createConnection();
    await assert.rejects(async () => {
      await connection.executeSql('SELECT * FROM InvalidTable');
    }, /InvalidTable/);

    await connection.close();
  });
});
