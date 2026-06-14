const { invokeAsync } = require('../grpc/utils');
const { Rows } = require('./rows');

class Connection {
    constructor(pool, connectionId) {
        this.pool = pool;
        this.connectionId = connectionId;
    }

    async execute(sql) {
        const executeRequest = {
            connection: {
                pool: { id: this.pool.poolId },
                id: this.connectionId
            },
            execute_sql_request: {
                sql: sql
            }
        };
        // 1. Execute the SQL to get a Rows identifier
        const rowsResult = await invokeAsync('execute', executeRequest);
        const rowsId = rowsResult.id;

        // 2. Create the request object for the metadata call
        const metadataRequest = {
            connection: {
                pool: { id: this.pool.poolId },
                id: this.connectionId
            },
            id: rowsId
        };
        // 3. Fetch the metadata to get column names
        const metadata = await invokeAsync('metadata', metadataRequest);
        
        // 4. Extract the column names from the metadata response
        const columnNames = metadata.row_type.fields.map(field => field.name);

        // 5. Return a new Rows object, now equipped with the column names
        return new Rows(this, rowsId, columnNames);
    }

    async close() {
        const request = {
             pool: { id: this.pool.poolId },
             id: this.connectionId
        };
        await invokeAsync('closeConnection', request);
        this.connectionId = null;
    }
}

module.exports = { Connection };
