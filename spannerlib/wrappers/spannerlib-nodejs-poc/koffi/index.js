const { spannerLib } = require('./src/lib/spannerlib.js');
const { Pool } = require('./src/lib/pool.js');
const { Connection } = require('./src/lib/connection.js');
const { Rows } = require('./src/lib/rows.js');
const { SpannerLibError } = require('./src/ffi/utils.js');

/**
 * Cleanup function to force release of all trapped Go CGO pinners
 * when the application shuts down.
 */
function cleanup() {
    spannerLib.releaseAll();
}

module.exports = {
    Pool,
    Connection,
    Rows,
    SpannerLibError,
    cleanup
};
