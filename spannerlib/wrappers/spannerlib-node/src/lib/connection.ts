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

import { ffi } from '../ffi/utils.js';
import { spannerLib } from './spannerlib.js';
import { Pool } from './pool.js';
import { Rows } from './rows.js';
// TODO: Avoid tight coupling to internal paths of full client libraries.
// Unlike other languages like Java, Python , Node client does not export its protos.
// We need to explore how to import protos in Node
import pkg from '@google-cloud/spanner/build/protos/protos.js';
const { google } = pkg;

/**
 * Manages a connection to the Spanner database.
 *
 * This class wraps the connection handle from the underlying Go library,
 * providing methods to execute SQL statements and manage transactions.
 */
export class Connection {
  public pool: Pool | null;
  public oid: number | null;
  public closed: boolean;

  /**
   * Creates a new connection within the specified pool.
   *
   * @param pool The pool to create the connection in.
   * @returns A Promise that resolves to a new Connection instance.
   * @throws {SpannerLibError} If creation fails in the Go library.
   */
  static async create(pool: Pool): Promise<Connection> {
    if (pool.closed || pool.oid === null) {
      throw new Error('Cannot create connection: Pool is closed or invalid');
    }
    const c = new Connection();
    c.pool = pool;

    const handled = await ffi.invokeAsync('CreateConnection', pool.oid);

    c.oid = handled.objectId;
    spannerLib.register(c, {
      type: 'connection',
      poolId: pool.oid,
      connectionId: c.oid!,
    });
    return c;
  }

  constructor() {
    this.pool = null;
    this.oid = null;
    this.closed = false;
  }

  /**
   * Executes a SQL statement on this connection.
   *
   * @param sqlString The SQL query string to execute.
   * @returns A Promise that resolves to a Rows instance containing results.
   * @throws {Error} If the connection is closed or not bound to a pool.
   * @throws {SpannerLibError} If execution fails in the Go library.
   */
  async executeSql(sqlString: string): Promise<Rows> {
    if (this.closed) throw new Error('Connection is already closed');
    if (!this.pool || this.pool.oid === null || this.oid === null) {
      throw new Error('Connection is not bound to a Pool or invalid');
    }

    const requestObj = { sql: sqlString, session: 'poc/dummy' };
    const ExecuteSqlRequestProto = google.spanner.v1.ExecuteSqlRequest;
    const serializedPb = Buffer.from(
      ExecuteSqlRequestProto.encode(requestObj).finish()
    );

    const rowsResult = await ffi.invokeAsync(
      'Execute',
      this.pool.oid,
      this.oid,
      serializedPb
    );
    const rowsId = rowsResult.objectId;

    return new Rows(this, rowsId);
  }

  /**
   * Closes the connection and releases associated resources.
   *
   * @returns A Promise that resolves when the connection is closed.
   */
  async close(): Promise<void> {
    if (!this.closed) {
      this.closed = true;
      try {
        if (
          this.pool &&
          !this.pool.closed &&
          this.pool.oid !== null &&
          this.oid !== null
        ) {
          await ffi.invokeAsync('CloseConnection', this.pool.oid, this.oid);
        }
      } finally {
        spannerLib.unregister(this);
      }
    }
  }
}
