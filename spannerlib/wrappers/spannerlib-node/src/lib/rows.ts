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

import { ffi, ENCODING_PROTOBUF } from '../ffi/utils.js';
import { spannerLib } from './spannerlib.js';
import { Connection } from './connection.js';
import { createRequire } from 'module';
// @ts-ignore
const _require = typeof require !== 'undefined' ? require : createRequire(import.meta.url);
const { google } = _require('@google-cloud/spanner/build/protos/protos.js');
const ListValue = google.protobuf.ListValue;



export class Rows {
    public connection: Connection;
    public oid: number;
    public pinnerId: number | null;
    public closed: boolean;
    constructor(connection: Connection, oid: number) {
        this.connection = connection;
        this.oid = oid;
        this.pinnerId = null;
        this.closed = false;
    }

    async next(): Promise<any> {
        if (this.closed) throw new Error("Rows are already closed");

        const handled = await ffi.invokeAsync(
            "Next",
            null,
            null,
            this.connection.pool!.oid,
            this.connection.oid,
            this.oid,
            1,
            ENCODING_PROTOBUF
        );

        if (!handled.protobufBytes || handled.protobufBytes.length === 0) {
            return null;
        }

        return ListValue.decode(handled.protobufBytes);
    }

    async close(): Promise<void> {
        if (!this.closed) {
            this.closed = true;
            try {
                await ffi.invokeAsync("CloseRows", this, spannerLib, this.connection.pool!.oid, this.connection.oid, this.oid);
            } finally {
                if (this.pinnerId !== null) {
                    spannerLib.unregister(this, this.pinnerId);
                }
            }
        }
    }
}
