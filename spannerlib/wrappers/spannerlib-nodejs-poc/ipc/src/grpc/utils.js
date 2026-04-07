const { client, messages } = require('./bindings');
const { Status } = require('@grpc/grpc-js/build/src/constants');

function invokeAsync(funcName, request) {
    return new Promise((resolve, reject) => {
        // Bind the method to the client instance to preserve `this` context.
        const method = client[funcName].bind(client);
        method(request, (err, response) => {
            if (err) {
                // gRPC returns an error object, not a non-zero status code on error.
                return reject(new Error(err.details || `gRPC Error Code: ${err.code}`));
            }
            resolve(response);
        });
    });
}

module.exports = {
    invokeAsync,
    messages
};
