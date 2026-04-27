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
import { createRequire } from 'module';
// @ts-ignore
const _require = typeof require !== 'undefined' ? require : createRequire(import.meta.url);
const { google } = _require('@google-cloud/spanner/build/protos/protos.js');

export class Connection {
    public pool: Pool | null;
    public oid: number | null;
    public pinnerId: number | null;
    public closed: boolean;

    static async create(pool: Pool): Promise<Connection> {
        const c = new Connection();
        c.pool = pool;

        const handled = await ffi.invokeAsync(
            "CreateConnection",
            c,
            spannerLib,
            pool.oid
        );

        c.oid = handled.objectId;
        c.pinnerId = handled.pinnerId;
        return c;
    }

    constructor() {
        this.pool = null;
        this.oid = null;
        this.pinnerId = null;
        this.closed = false;
    }

    async executeSql(sqlString: string): Promise<Rows> {
        if (this.closed) throw new Error("Connection is already closed");
        if (!this.pool) throw new Error("Connection is not bound to a Pool");

        const requestObj = { sql: sqlString, session: "poc/dummy" };
        const ExecuteSqlRequestProto = google.spanner.v1.ExecuteSqlRequest;
        const serializedPb = ExecuteSqlRequestProto.encode(requestObj).finish();

        const rowsResult = await ffi.invokeAsync(
            "Execute",
            null,
            null,
            this.pool.oid,
            this.oid,
            serializedPb
        );
        const rowsId = rowsResult.objectId;

        return new Rows(this, rowsId);
    }

    async close(): Promise<void> {
        if (!this.closed) {
            this.closed = true;
            try {
                if (this.pool && this.oid !== null) {
                    await ffi.invokeAsync("CloseConnection", this, spannerLib, this.pool.oid, this.oid);
                }
            } finally {
                if (this.pinnerId !== null) {
                    spannerLib.unregister(this, this.pinnerId);
                }
            }
        }
    }
}
