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

import { invokeAsync } from '../ffi/utils.js';
import { spannerLib } from './spannerlib.js';
import { Connection } from './connection.js';

export class Pool {
    public oid: number | null;
    public pinnerId: number | null;
    public closed: boolean;
    public userAgent: string;
    public connStr: string;

    static async create(connectionString: string): Promise<Pool> {
        // Detect if running in ESM context
        const isESM = typeof require === 'undefined';
        const userAgentSuffix = isESM ? 'node-esm' : 'node-cjs';

        const p = new Pool(userAgentSuffix, connectionString);
        const handled = await invokeAsync(
            "CreatePool",
            p,
            spannerLib,
            userAgentSuffix,
            connectionString
        );

        p.oid = handled.objectId;
        p.pinnerId = handled.pinnerId;
        return p;
    }

    constructor(userAgent: string, connectionString: string) {
        this.oid = null;
        this.pinnerId = null;
        this.closed = false;
        this.userAgent = userAgent;
        this.connStr = connectionString;
    }

    async createConnection(): Promise<Connection> {
        if (this.closed) throw new Error("Pool is already closed");
        return await Connection.create(this);
    }

    async close(): Promise<void> {
        if (!this.closed) {
            this.closed = true;
            try {
                if (this.oid !== null) {
                    await invokeAsync("ClosePool", this, spannerLib, this.oid);
                }
            } finally {
                if (this.pinnerId !== null) {
                    spannerLib.unregister(this, this.pinnerId);
                }
            }
        }
    }
}
