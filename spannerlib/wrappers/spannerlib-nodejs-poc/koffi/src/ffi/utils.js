const { koffi } = require('./bindings.js');

const ENCODING_JSON = 0;
const ENCODING_PROTOBUF = 1;

class SpannerLibError extends Error {
    constructor(code, message) {
        super(`Spanner Native Error (Code ${code}): ${message}`);
        this.code = code;
        this.name = 'SpannerLibError';
    }
}

function toGoString(str) {
    if (!str) return { p: null, n: 0 };
    return { p: str, n: Buffer.byteLength(str, 'utf8') };
}

function toGoSlice(bufOrStr) {
    const buf = Buffer.isBuffer(bufOrStr) ? bufOrStr : Buffer.from(bufOrStr, 'utf8');
    return { data: buf, len: buf.length, cap: buf.length };
}

/**
 * Validates the raw Go Return Tuple and safely decodes `resPointer` Protobuf bytes.
 * Throws wrapped JavaScript Errors smoothly cleanly.
 */
function handleGoResult(result, refInstance, pinManager) {
    // 1. Use FinalizationRegistry to register Go Memory bound to this JS Object!
    if (result && result.r0 > 0 && pinManager && refInstance) {
        pinManager.register(refInstance, result.r0);
        refInstance.pinnerId = result.r0; // Store it safely on the object it belongs to
    }
    
    let msg = "Unknown protobuf payload";
    let decodedBytes = Buffer.alloc(0);

    // We expect r4 to contain the protobuf response.
    if (result && result.r4 !== null && result.r3 > 0) {
        decodedBytes = Buffer.from(koffi.decode(result.r4, 'uint8', result.r3));
        msg = decodedBytes.toString('utf8');
    }

    if (result && result.r1 !== 0) {
        throw new SpannerLibError(result.r1, msg);
    }
    
    // Returns objectId (r2) and the raw binary (Protobuf)
    return { objectId: result.r2, protobufBytes: decodedBytes, message: msg };
}

/**
 * Promisifies asynchronous Koffi executions to prevent Node.js main thread blocking.
 */
function invokeAsync(koffiFunc, refInstance, pinManager, ...args) {
    return new Promise((resolve, reject) => {
        // Execute Koffi FFI on a background libuv thread natively.
        koffiFunc.async(...args, (err, resultTuple) => {
            if (err) {
                return reject(err);
            }
            try {
                // Now safely process the FFI response and register it for memory tracking
                resolve(handleGoResult(resultTuple, refInstance, pinManager));
            } catch (goError) {
                reject(goError);
            }
        });
    });
}

module.exports = {
    ENCODING_JSON,
    ENCODING_PROTOBUF,
    SpannerLibError,
    toGoString,
    toGoSlice,
    invokeAsync
};
