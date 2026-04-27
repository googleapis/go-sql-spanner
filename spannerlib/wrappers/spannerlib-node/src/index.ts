import { spannerLib } from './lib/spannerlib.js';
import { Pool } from './lib/pool.js';
import { Connection } from './lib/connection.js';
import { Rows } from './lib/rows.js';
import { SpannerLibError } from './ffi/utils.js';

export function cleanup(): void {
    spannerLib.releaseAll();
}

export {
    Pool,
    Connection,
    Rows,
    SpannerLibError
};
