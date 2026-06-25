const { invokeAsync } = require('../grpc/utils');
const { Connection } = require('./connection');

class Pool {
    constructor(poolId) {
        this.poolId = poolId;
    }

    static async create(connectionString, userAgent) {
        const request = {
            connection_string: connectionString,
            user_agent_suffix: userAgent,
        };
        const res = await invokeAsync('createPool', request);
        return new Pool(res.id);
    }

    async createConnection() {
        const request = {
            pool: { id: this.poolId }
        };
        const res = await invokeAsync('createConnection', request);
        return new Connection(this, res.id);
    }

    async close() {
        const request = { id: this.poolId };
        await invokeAsync('closePool', request);
        this.poolId = null;
    }
}

module.exports = { Pool };
