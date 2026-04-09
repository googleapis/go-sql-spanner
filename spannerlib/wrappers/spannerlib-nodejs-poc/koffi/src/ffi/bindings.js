const koffi = require('koffi');
const path = require('path');

// Dynamically locate the actual compiled Go shared library
const libPath = path.resolve(__dirname, '../../../../../shared/libspanner.so');
const lib = koffi.load(libPath);

// --- Define C-ABI Go Types ---
const GoString = koffi.struct('GoString', {
    p: 'string', // Maps to const char*
    n: 'size_t'  // Maps to ptrdiff_t
});

const GoSlice = koffi.struct('GoSlice', {
    data: 'void*', 
    len: 'int64',
    cap: 'int64'
});

const GoReturnTuple = koffi.struct('GoReturnTuple', {
    r0: 'int64',    // pinnerId
    r1: 'int32',    // statusCode
    r2: 'int64',    // objectId
    r3: 'int32',    // length
    r4: 'void*'     // resPointer
});

// --- Function Bindings ---
module.exports = {
    koffi,
    lib,
    types: {
        GoString,
        GoSlice,
        GoReturnTuple
    },
    // FFI Native Functions
    CreatePool: lib.func('CreatePool', GoReturnTuple, [GoString, GoString]),
    ClosePool: lib.func('ClosePool', GoReturnTuple, ['int64']),
    CreateConnection: lib.func('CreateConnection', GoReturnTuple, ['int64']),
    CloseConnection: lib.func('CloseConnection', GoReturnTuple, ['int64', 'int64']),
    Execute: lib.func('Execute', GoReturnTuple, ['int64', 'int64', GoSlice]),
    Metadata: lib.func('Metadata', GoReturnTuple, ['int64', 'int64', 'int64']),
    Next: lib.func('Next', GoReturnTuple, ['int64', 'int64', 'int64', 'int32', 'int32']),
    CloseRows: lib.func('CloseRows', GoReturnTuple, ['int64', 'int64', 'int64']),
    Release: lib.func('Release', 'int32', ['int64'])
};
