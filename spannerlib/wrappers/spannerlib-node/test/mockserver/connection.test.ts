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
import pkg from '@google-cloud/spanner/build/protos/protos.js';
const { google } = pkg;

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

    const rows = await connection.execute('SELECT 1');
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
    const rows = await connection.execute('SELECT * FROM AllTypes');
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
      connections.map((c) => c.execute('SELECT 1'))
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
      await connection.execute('SELECT * FROM InvalidTable');
    }, /InvalidTable/);

    await connection.close();
  });

  it('should manage explicit transaction lifecycle (beginTransaction, commit)', async () => {
    mock.putStatementResult(
      'SELECT 1',
      StatementResult.resultSet(createSelect1ResultSet())
    );

    const connection = await pool.createConnection();
    await connection.beginTransaction();

    const rows = await connection.execute('SELECT 1');
    const row: any = await rows.next();
    assert.strictEqual(row.values[0].stringValue, '1');
    await rows.close();

    const commitResp = await connection.commit();
    assert.ok(commitResp, 'Commit response returned');
    assert.ok(commitResp.commitTimestamp, 'Commit timestamp present');

    await connection.close();
  });

  it('should manage explicit transaction rollback (beginTransaction, rollback)', async () => {
    const connection = await pool.createConnection();
    await connection.beginTransaction();
    await connection.rollback();
    await connection.close();
  });

  it('should write mutations successfully', async () => {
    const connection = await pool.createConnection();
    const Mutation = google.spanner.v1.Mutation;
    const mutation = Mutation.create({
      insert: {
        table: 'Users',
        columns: ['id', 'name'],
        values: [{ values: [{ stringValue: '1' }, { stringValue: 'Alice' }] }],
      },
    });
    const res = await connection.writeMutations([mutation]);
    assert.ok(res, 'CommitResponse returned');
    assert.ok(res.commitTimestamp, 'Commit timestamp present');
    await connection.close();
  });

  it('should execute executeBatch successfully', async () => {
    mock.putStatementResult(
      'UPDATE Users SET status = "ACTIVE" WHERE id = 1',
      StatementResult.updateCount(1)
    );

    const connection = await pool.createConnection();
    const res = await connection.executeBatch([
      'UPDATE Users SET status = "ACTIVE" WHERE id = 1',
    ]);
    assert.ok(res, 'ExecuteBatchDmlResponse returned');
    assert.strictEqual(
      Number(res.resultSets![0].stats!.rowCountExact),
      1,
      'Row count matches'
    );
    await connection.close();
  });

  it('should support execute() and executeBatch() aliases matching Java/Python wrappers', async () => {
    mock.putStatementResult(
      'SELECT 1',
      StatementResult.resultSet(createSelect1ResultSet())
    );
    mock.putStatementResult(
      'UPDATE Users SET status = "ACTIVE" WHERE id = 1',
      StatementResult.updateCount(1)
    );

    const connection = await pool.createConnection();

    const ExecuteSqlRequest = google.spanner.v1.ExecuteSqlRequest;
    const rows = await connection.execute(
      ExecuteSqlRequest.create({ sql: 'SELECT 1' })
    );
    const row: any = await rows.next();
    assert.strictEqual(row.values[0].stringValue, '1');
    await rows.close();

    const ExecuteBatchDmlRequest = google.spanner.v1.ExecuteBatchDmlRequest;
    const res = await connection.executeBatch(
      ExecuteBatchDmlRequest.create({
        statements: [
          { sql: 'UPDATE Users SET status = "ACTIVE" WHERE id = 1' },
        ],
      })
    );
    assert.strictEqual(Number(res.resultSets![0].stats!.rowCountExact), 1);

    await connection.close();
  });

  it('should execute read-only transaction successfully', async () => {
    mock.putStatementResult(
      'SELECT 1',
      StatementResult.resultSet(createSelect1ResultSet())
    );

    const connection = await pool.createConnection();
    const TransactionOptions = google.spanner.v1.TransactionOptions;
    await connection.beginTransaction(
      TransactionOptions.create({
        readOnly: { strong: true },
      })
    );

    const rows = await connection.execute('SELECT 1');
    const row: any = await rows.next();
    assert.strictEqual(row.values[0].stringValue, '1');
    await rows.close();

    await connection.commit();

    await connection.close();
  });

  it('should buffer mutations when an active transaction exists', async () => {
    const connection = await pool.createConnection();
    await connection.beginTransaction();

    const Mutation = google.spanner.v1.Mutation;
    const mutation = Mutation.create({
      insert: {
        table: 'Users',
        columns: ['id', 'name'],
        values: [{ values: [{ stringValue: '2' }, { stringValue: 'Bob' }] }],
      },
    });
    const res = await connection.writeMutations([mutation]);
    assert.strictEqual(res, null, 'Buffered mutation returns null/nil');

    const commitResp = await connection.commit();
    assert.ok(commitResp, 'Commit response returned upon commit');
    assert.ok(commitResp.commitTimestamp, 'Commit timestamp present');

    await connection.close();
  });

  it('should execute query with parameters successfully', async () => {
    mock.putStatementResult(
      'SELECT FirstName FROM Singers WHERE SingerId = @id',
      StatementResult.resultSet(createSelect1ResultSet())
    );

    const connection = await pool.createConnection();
    const ExecuteSqlRequest = google.spanner.v1.ExecuteSqlRequest;
    const rows = await connection.execute(
      ExecuteSqlRequest.create({
        sql: 'SELECT FirstName FROM Singers WHERE SingerId = @id',
        params: { fields: { id: { stringValue: '50' } } },
        paramTypes: { id: { code: 'INT64' } },
      })
    );
    const row: any = await rows.next();
    assert.strictEqual(row.values[0].stringValue, '1');
    await rows.close();
    await connection.close();
  });

  it('should throw error when calling beginTransaction twice', async () => {
    const connection = await pool.createConnection();
    await connection.beginTransaction();
    await assert.rejects(async () => {
      await connection.beginTransaction();
    });
    await connection.rollback();
    await connection.close();
  });
});
