const { CloseRows, Next } = require('../ffi/bindings.js');
const { invokeAsync } = require('../ffi/utils.js');
const { spannerLib } = require('./spannerlib.js');
const { google } = require('@google-cloud/spanner/build/protos/protos.js');

class Rows {
    /**
     * @param {import('./connection.js').Connection} conn 
     * @param {Number} objectId - The OID identifying these rows inside the Go Driver
     */
    constructor(conn, objectId) {
        this.conn = conn;

        /**
         * The Object ID (OID). 
         * Used to execute operations like `.next()` against THIS specific ResultSet inside Go.
         * @type {Number|null}
         */
        this.oid = objectId;

        this.closed = false;
        
        // FinalizationRegistry could optionally be mapped to this via 
        // spannerLib.register(this, pinnerId_from_execute)
        // For the POC, we won't fully map the Rows Pinner unless needed
        // by the Next() function iterator.
    }

    /**
     * Iterates to the next result chunk.
     * In a full implementation, it would call `Next(poolId, connId, rowsId, ...)` natively.
     */
    async next() {
        if (this.closed) throw new Error("Rows object is already closed");

        // Go Signature: Next(poolId, connId, rowsId, numRows, encodeRowOption) 
        // We pass 1 for numRows, and 0 for encodeRowOption as per POC defaults
        const handled = await invokeAsync(Next, null, null, this.conn.pool.oid, this.conn.oid, this.oid, 1, 0);

        // Handle EOF case (The chunk buffer is perfectly empty or contains no message)
        if (!handled.protobufBytes || handled.protobufBytes.length === 0) {
            return null; // Signals end of rows to the caller
        }

        // The returned message contains a google.protobuf.ListValue according to the spec!
        // We natively unpack those bytes matching standard Protobuf conventions.
        const listValueProto = google.protobuf.ListValue;
        const decodedList = listValueProto.decode(handled.protobufBytes);

        // This converts the complex generic Protobuf ListValue deeply into native Javascript!
        const jsonRecord = listValueProto.toObject(decodedList, {
            longs: String, // Ensure Int64 types from Spanner decode as Strings instead of mangled JS doubles
            enums: String,
            bytes: String,
        });

        return jsonRecord.values || [];
    }

    /**
     * Closes the rows object safely, waiting on the network.
     */
    async close() {
        if (!this.closed) {
            this.closed = true;
            // Native FFI execution in the background
            await invokeAsync(CloseRows, null, spannerLib, this.conn.pool.oid, this.conn.oid, this.oid);
        }
    }
}

module.exports = { Rows };
