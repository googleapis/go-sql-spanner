import { invokeAsync } from '../ffi/utils.js';
import { spannerLib } from './spannerlib.js';
import { Connection } from './connection.js';

export class Pool {
    public oid: number | null;
    public pinnerId: number | null;
    public closed: boolean;
    public userAgent: string;
    public connStr: string;

    static async create(userAgent: string, connectionString: string): Promise<Pool> {
        const p = new Pool(userAgent, connectionString);
        const handled = await invokeAsync(
            "CreatePool",
            p,
            spannerLib,
            userAgent,
            connectionString
        );

        p.oid = handled.objectId;
        p.pinnerId = handled.pinnerId;
        return p;
    }

    constructor(userAgent: string, connectionString: string) {
        this.oid = null;
        this.pinnerId = null;
        this.closed = false;
        this.userAgent = userAgent;
        this.connStr = connectionString;
    }

    async createConnection(): Promise<Connection> {
        if (this.closed) throw new Error("Pool is already closed");
        return await Connection.create(this);
    }

    async close(): Promise<void> {
        if (!this.closed) {
            this.closed = true;
            try {
                if (this.oid !== null) {
                    await invokeAsync("ClosePool", this, spannerLib, this.oid);
                }
            } finally {
                if (this.pinnerId !== null) {
                    spannerLib.unregister(this, this.pinnerId);
                }
            }
        }
    }
}
