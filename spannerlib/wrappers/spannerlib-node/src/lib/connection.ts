import { invokeAsync } from '../ffi/utils.js';
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

        const handled = await invokeAsync(
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

        const rowsResult = await invokeAsync(
            "Execute",
            null,
            null,
            this.pool.oid,
            this.oid,
            serializedPb
        );
        const rowsId = rowsResult.objectId;

        const metadataResult = await invokeAsync(
            "Metadata",
            null,
            null,
            this.pool.oid,
            this.oid,
            rowsId
        );
        
        const ResultSetMetadataProto = google.spanner.v1.ResultSetMetadata;
        const metadata = ResultSetMetadataProto.decode(metadataResult.protobufBytes);
        const columnInfo = metadata.rowType.fields.map((field: any) => ({
            name: field.name,
            typeCode: field.type.code
        }));
        
        return new Rows(this, rowsId, columnInfo);
    }

    async close(): Promise<void> {
        if (!this.closed) {
            this.closed = true;
            try {
                if (this.pool && this.oid !== null) {
                    await invokeAsync("CloseConnection", this, spannerLib, this.pool.oid, this.oid);
                }
            } finally {
                if (this.pinnerId !== null) {
                    spannerLib.unregister(this, this.pinnerId);
                }
            }
        }
    }
}
