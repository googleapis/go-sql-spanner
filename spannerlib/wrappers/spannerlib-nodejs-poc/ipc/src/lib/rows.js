const { invokeAsync } = require('../grpc/utils');

const ENCODING_PROTOBUF = 1;

/**
 * Parses a `ListValue` plain object and a list of column names into a
 * single key-value row object.
 * @param {object} listValue The ListValue object from the gRPC response.
 * @param {string[]} columnNames An array of column names from the metadata.
 * @returns {object|null} A key-value object representing the row, or null.
 */
function parseRowToObject(listValue, columnNames) {
    if (!listValue || !listValue.values || listValue.values.length === 0) {
        return null; // This signifies the end of the result set.
    }

    const rowObject = {};
    listValue.values.forEach((value, index) => {
        const columnName = columnNames[index];
        const kind = value.kind;
        let parsedValue;

        switch (kind) {
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
    constructor(connection, rowsId, columnNames) {
        this.connection = connection;
        this.rowsId = rowsId;
        this.columnNames = columnNames; // Store the column names
    }

    /**
     * Fetches the next row from the result set.
     * @returns {Promise<object|null>} A promise that resolves to a row object, or null if there are no more rows.
     */
    async next() {
        const request = {
            rows: {
                connection: {
                    pool: { id: this.connection.pool.poolId },
                    id: this.connection.connectionId
                },
                id: this.rowsId
            },
            fetch_options: {
                num_rows: 1, // Always fetch one row at a time.
                encoding: ENCODING_PROTOBUF
            }
        };
        const res = await invokeAsync('next', request);
        return parseRowToObject(res, this.columnNames);
    }

    async close() {
        const request = {
            connection: {
                pool: { id: this.connection.pool.poolId },
                id: this.connection.connectionId
            },
            id: this.rowsId
        };
        await invokeAsync('closeRows', request);
        this.rowsId = null;
    }
}

module.exports = { Rows };
