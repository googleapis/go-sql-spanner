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

import * as assert from 'assert';
import { Connection } from '../src/lib/connection.js';
import { Pool } from '../src/lib/pool.js';
import { ffi } from '../src/ffi/utils.js';
import sinon from 'sinon';

describe('Connection', () => {
    let stub: sinon.SinonStub;

    beforeEach(() => {
        stub = sinon.stub(ffi, 'invokeAsync');
    });

    afterEach(() => {
        stub.restore();
    });

    it('should execute SQL successfully', async () => {
        // Mock Pool
        const pool = new Pool('node-esm', 'projects/test/instances/test/databases/test');
        pool.oid = 1;

        const connection = new Connection();
        connection.pool = pool;
        connection.oid = 2;

        // Mock invokeAsync response for Execute
        stub.onFirstCall().resolves({ objectId: 3, pinnerId: 0, protobufBytes: null });

        const rows = await connection.executeSql("SELECT 1");

        assert.ok(rows, 'Rows should be returned');
        assert.strictEqual(rows.oid, 3, 'Rows OID should match');
        assert.strictEqual(rows.connection, connection, 'Rows connection should match');
        
        assert.strictEqual(stub.calledOnce, true, 'invokeAsync should be called once');
        assert.strictEqual(stub.firstCall.args[0], 'Execute', 'First call should be Execute');
    });

    it('should throw error if connection is closed', async () => {
        const connection = new Connection();
        connection.closed = true;

        await assert.rejects(
            async () => {
                await connection.executeSql("SELECT 1");
            },
            /Connection is already closed/
        );
    });

    it('should throw error if pool is not bound', async () => {
        const connection = new Connection();

        await assert.rejects(
            async () => {
                await connection.executeSql("SELECT 1");
            },
            /Connection is not bound to a Pool/
        );
    });
});
