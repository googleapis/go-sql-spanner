const { CreateConnection, CloseConnection, Execute } = require('../ffi/bindings.js');
const { invokeAsync } = require('../ffi/utils.js');
const { spannerLib } = require('./spannerlib.js');
const { Rows } = require('./rows.js');
const { spanner_v1 } = require('@google-cloud/spanner/build/protos/protos.js');

class Connection {
    /**
     * @param {import('./pool.js').Pool} pool 
     */
    static async create(pool) {
        const c = new Connection();
        c.pool = pool;

        const handled = await invokeAsync(
            CreateConnection,
            c,             // Self reference for GC memory watcher
            spannerLib,    // Watcher
            pool.oid
        );

        c.oid = handled.objectId;
        return c;
    }

    constructor() {
        this.pool = null;

        /**
         * The Object ID (OID). 
         * The global identifier for this specific Connection inside the Go engine.
         * @type {Number|null}
         */
        this.oid = null;

        /**
         * The Memory Pinner ID.
         * The exact GC lock holding this Connection's memory intact in Go.
         * We pass this to native `Release()` via the Registry to stop leaks.
         * @type {Number|null}
         */
        this.pinnerId = null;

        this.closed = false;
    }

    /**
     * Natively executes a SQL query on Node's background LibUV thread while V8 proceeds!
     * Integrates raw JS Protobuf definitions (from @google-cloud/spanner package).
     */
    async executeSql(sqlString) {
        if (this.closed) throw new Error("Connection is already closed");

        const requestObj = { sql: sqlString, session: "poc/dummy" };
        const { google } = require('@google-cloud/spanner/build/protos/protos.js');
        const ExecuteSqlRequestProto = google.spanner.v1.ExecuteSqlRequest;
        const serializedPb = ExecuteSqlRequestProto.encode(requestObj).finish();

        // 1. Execute the SQL to get a Rows identifier
        const rowsResult = await invokeAsync(
            "Execute",
            null,
            null,
            this.pool.oid,
            this.oid,
            serializedPb
        );
        const rowsId = rowsResult.objectId;

        // 2. Fetch the metadata to get column names
        const metadataResult = await invokeAsync(
            "Metadata",
            null,
            null,
            this.pool.oid,
            this.oid,
            rowsId
        );
        
        // 3. Decode the metadata protobuf
        const ResultSetMetadataProto = google.spanner.v1.ResultSetMetadata;
        const metadata = ResultSetMetadataProto.decode(metadataResult.protobufBytes);
        const columnInfo = metadata.rowType.fields.map(field => ({
            name: field.name,
            typeCode: field.type.code
        }));
        
        // 4. Return a new Rows object, now equipped with the column info
        return new Rows(this, rowsId, columnInfo);
    }

    /**
     * Release the connection back to the pool asynchronously
     */
    async close() {
        if (!this.closed) {
            this.closed = true;
            try {
                await invokeAsync(CloseConnection, this, spannerLib, this.pool.oid, this.oid);
            } finally {
                spannerLib.unregister(this, this.pinnerId);
            }
        }
    }
}

module.exports = { Connection };
