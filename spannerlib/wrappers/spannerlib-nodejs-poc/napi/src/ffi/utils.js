const addon = require('../../build/Release/spanner_napi.node');

const ENCODING_JSON = 0;
const ENCODING_PROTOBUF = 1;

/**
 * Normalizes C++ responses to look structurally identical to what Koffi did.
 * Koffi returned: { r0: int, r1: int, r2: int, r3: int, r4: pointer/buffer }
 */
function invokeAsync(funcName, constructor1, constructor2, ...args) {
    return new Promise((resolve, reject) => {
        const callback = (err, result) => {
            if (err) {
                return reject(err);
            }
            // Parse Go Error Codes identically to Koffi
            if (result.r1 !== 0) {
                if (result.r4 && result.r3 > 0) {
                    const errorJson = result.r4.toString('utf8');
                    try {
                        const parsed = JSON.parse(errorJson);
                        return reject(new Error(parsed.message || errorJson));
                    } catch (e) {
                        return reject(new Error(errorJson));
                    }
                }
                return reject(new Error(`Native Spanner Error Code: ${result.r1}`));
            }

            // Normal Execution Success
            resolve({
                objectId: result.r2, // The new OID (Rows OID, Connection OID)
                pinnerId: result.r0, // The GC lock
                protobufBytes: result.r4 // The protobuf payload
            });
        };

        // Call the C++ add-on method with arguments and callback
        addon[funcName](...args, callback);
    });
}

/**
 * Synchronous direct export for GC memory release
 */
const Release = addon.Release;

module.exports = {
    ENCODING_JSON,
    ENCODING_PROTOBUF,
    invokeAsync,
    Release
};
