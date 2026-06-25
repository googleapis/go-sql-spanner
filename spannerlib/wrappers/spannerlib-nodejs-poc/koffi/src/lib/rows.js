const { Next, CloseRows } = require('../ffi/bindings.js');
const { invokeAsync, ENCODING_PROTOBUF } = require('../ffi/utils.js');
const { spannerLib } = require('./spannerlib.js');
const { google } = require('@google-cloud/spanner/build/protos/protos.js');
const ListValue = google.protobuf.ListValue;
const Value = google.protobuf.Value;

/**
 * Parses a binary `ListValue` protobuf message into a key-value row object,
 * using type information from the query metadata.
 * @param {Buffer} buffer The binary buffer from the Go library.
 * @param {Array<{name: string, typeCode: number}>} columnInfo An array of objects with column names and types.
 * @returns {object|null}
 */
function parseRowToObject(buffer, columnInfo) {
    if (!buffer || buffer.length === 0) {
        return null; // End of result set
    }

    const listValue = ListValue.decode(buffer);
    const rowObject = {};
    const values = listValue.values;

    columnInfo.forEach((column, index) => {
        const value = values[index];
        const columnName = column.name;
        let parsedValue;

        // The decoded `value` object has a 'kind' oneof field.
        // We check which property is set to get the primitive value.
        switch (value.kind) {
            case 'nullValue':
                parsedValue = null;
                break;
            case 'numberValue':
                parsedValue = value.numberValue;
                break;
            case 'stringValue':
                parsedValue = value.stringValue;
                break;
            case 'boolValue':
                parsedValue = value.boolValue;
                break;
            default:
                parsedValue = undefined;
        }
        rowObject[columnName] = parsedValue;
    });

    return rowObject;
}

class Rows {
    /**
     * @param {import('./connection.js').Connection} connection
     * @param {Number} oid
     * @param {Array<{name: string, typeCode: number}>} columnInfo
     */
    constructor(connection, oid, columnInfo) {
        this.connection = connection;
        this.oid = oid;
        this.pinnerId = null;
        this.closed = false;
        this.columnInfo = columnInfo; // Store column names and types
    }

    /**
     * Fetches the next row from the result set.
     * @returns {Promise<object|null>} A promise that resolves to a row object, or null if there are no more rows.
     */
    async next() {
        if (this.closed) throw new Error("Rows are already closed");

        const handled = await invokeAsync(
            Next,
            null,
            null,
            this.connection.pool.oid,
            this.connection.oid,
            this.oid,
            1, // Fetch one row at a time
            ENCODING_PROTOBUF
        );

        // The result for `Next` is a binary `ListValue` protobuf message.
        return parseRowToObject(handled.protobufBytes, this.columnInfo);
    }

    async close() {
        if (!this.closed) {
            this.closed = true;
            try {
                await invokeAsync(CloseRows, this, spannerLib, this.connection.pool.oid, this.connection.oid, this.oid);
            } finally {
                spannerLib.unregister(this, this.pinnerId);
            }
        }
    }
}

module.exports = { Rows };
