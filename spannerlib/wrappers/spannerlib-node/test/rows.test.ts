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
import { Rows } from '../src/lib/rows.js';
import { Connection } from '../src/lib/connection.js';
import { Pool } from '../src/lib/pool.js';
import { ffi } from '../src/ffi/utils.js';
import sinon from 'sinon';
import pkg from '@google-cloud/spanner/build/protos/protos.js';
const { google } = pkg;
const ListValue = google.protobuf.ListValue;
const Value = google.protobuf.Value;

describe('Rows', () => {
  let stub: sinon.SinonStub;

  beforeEach(() => {
    stub = sinon.stub(ffi, 'invokeAsync');
  });

  afterEach(() => {
    stub.restore();
  });

  interface MockListValue {
    values: Array<{ stringValue: string }>;
  }

  it('should fetch next row successfully', async () => {
    const pool = new Pool(
      'node-esm',
      'projects/test/instances/test/databases/test'
    );
    pool.oid = 1;
    const connection = new Connection();
    connection.pool = pool;
    connection.oid = 2;

    const rows = new Rows(connection, 3);

    // Create a dummy ListValue
    const listValue = ListValue.create({
      values: [Value.create({ stringValue: '1' })],
    });
    const buffer = ListValue.encode(listValue).finish() as Buffer;

    stub
      .onFirstCall()
      .resolves({ objectId: 0, pinnerId: 0, protobufBytes: buffer });

    const row = (await rows.next()) as unknown as MockListValue;

    assert.ok(row, 'Row should be returned');
    assert.strictEqual(row.values.length, 1, 'Row should have 1 value');
    assert.strictEqual(row.values[0].stringValue, '1', 'Value should be "1"');

    assert.strictEqual(
      stub.calledOnce,
      true,
      'invokeAsync should be called once'
    );
    assert.strictEqual(
      stub.firstCall.args[0],
      'Next',
      'First call should be Next'
    );
  });

  it('should return null when no more rows', async () => {
    const pool = new Pool(
      'node-esm',
      'projects/test/instances/test/databases/test'
    );
    pool.oid = 1;
    const connection = new Connection();
    connection.pool = pool;
    connection.oid = 2;

    const rows = new Rows(connection, 3);

    stub
      .onFirstCall()
      .resolves({ objectId: 0, pinnerId: 0, protobufBytes: null });

    const row = await rows.next();

    assert.strictEqual(row, null, 'Row should be null');
  });
});
