import * as assert from 'assert';
import { Pool, Connection, Rows } from '../src/index.js';

describe('SpannerLib Node Wrapper', () => {
    it('should correctly export the primary interface classes', () => {
        assert.ok(Pool, 'Pool class should be exported');
        assert.ok(Connection, 'Connection class should be exported');
        assert.ok(Rows, 'Rows class should be exported');
    });

    it('should instantiate a Pool object properly', () => {
        const pool = new Pool('test-agent', 'projects/test/instances/test/databases/test');
        assert.strictEqual(pool.userAgent, 'test-agent');
        assert.strictEqual(pool.connStr, 'projects/test/instances/test/databases/test');
    });
});
