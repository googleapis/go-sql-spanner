/**
 * In N-API, there is no need for 'bindings' mapping types like 'int64' and 'pointer' 
 * because the C++ addon already handles all V8 type conversions directly.
 * We simply export the names of the C++ functions that the wrappers will use.
 */
module.exports = {
  CreatePool: 'CreatePool',
  ClosePool: 'ClosePool',
  CreateConnection: 'CreateConnection',
  CloseConnection: 'CloseConnection',
  Execute: 'Execute',
  Next: 'Next',
  CloseRows: 'CloseRows'
};
