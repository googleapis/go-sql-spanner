#include <napi.h>
#include <iostream>
#include "../../../../../shared/libspanner.h"

//
// Worker 1: CreatePool asynchronously
//
class CreatePoolWorker : public Napi::AsyncWorker {
public:
    CreatePoolWorker(Napi::Function& callback, std::string userAgent, std::string connStr)
        : AsyncWorker(callback), userAgent_(userAgent), connStr_(connStr), result_({0, 0, 0, 0, nullptr}) {}

    void Execute() override {
        GoString goUserAgent = {userAgent_.c_str(), (ptrdiff_t)userAgent_.length()};
        GoString goConnStr = {connStr_.c_str(), (ptrdiff_t)connStr_.length()};
        result_ = CreatePool(goUserAgent, goConnStr);
    }

    void OnOK() override {
        Napi::Env env = Env();
        Napi::Object obj = Napi::Object::New(env);
        obj.Set("r0", Napi::Number::New(env, result_.r0));
        obj.Set("r1", Napi::Number::New(env, result_.r1));
        obj.Set("r2", Napi::Number::New(env, result_.r2));
        obj.Set("r3", Napi::Number::New(env, result_.r3));
        
        if (result_.r4 != nullptr && result_.r3 > 0) {
            obj.Set("r4", Napi::Buffer<uint8_t>::Copy(env, (uint8_t*)result_.r4, result_.r3));
        } else {
            obj.Set("r4", env.Null());
        }
        Callback().Call({env.Null(), obj});
    }

private:
    std::string userAgent_;
    std::string connStr_;
    CreatePool_return result_;
};

Napi::Value CreatePoolWrapper(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    std::string ua = info[0].As<Napi::String>();
    std::string cs = info[1].As<Napi::String>();
    Napi::Function cb = info[2].As<Napi::Function>();
    CreatePoolWorker* worker = new CreatePoolWorker(cb, ua, cs);
    worker->Queue();
    return env.Undefined();
}

//
// Worker 2: ClosePool asynchronously
//
class ClosePoolWorker : public Napi::AsyncWorker {
public:
    ClosePoolWorker(Napi::Function& callback, int64_t poolId)
        : AsyncWorker(callback), poolId_(poolId), result_({0, 0, 0, 0, nullptr}) {}

    void Execute() override {
        result_ = ClosePool(poolId_);
    }

    void OnOK() override {
        Napi::Env env = Env();
        Napi::Object obj = Napi::Object::New(env);
        obj.Set("r1", Napi::Number::New(env, result_.r1));
        Callback().Call({env.Null(), obj});
    }
private:
    int64_t poolId_;
    ClosePool_return result_;
};

Napi::Value ClosePoolWrapper(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    int64_t pid = info[0].As<Napi::Number>().Int64Value();
    Napi::Function cb = info[1].As<Napi::Function>();
    ClosePoolWorker* worker = new ClosePoolWorker(cb, pid);
    worker->Queue();
    return env.Undefined();
}

//
// Worker 3: CreateConnection asynchronously
//
class CreateConnectionWorker : public Napi::AsyncWorker {
public:
    CreateConnectionWorker(Napi::Function& callback, int64_t poolId)
        : AsyncWorker(callback), poolId_(poolId), result_({0, 0, 0, 0, nullptr}) {}

    void Execute() override {
        result_ = CreateConnection(poolId_);
    }

    void OnOK() override {
        Napi::Env env = Env();
        Napi::Object obj = Napi::Object::New(env);
        obj.Set("r0", Napi::Number::New(env, result_.r0));
        obj.Set("r1", Napi::Number::New(env, result_.r1));
        obj.Set("r2", Napi::Number::New(env, result_.r2));
        obj.Set("r3", Napi::Number::New(env, result_.r3));
        
        if (result_.r4 != nullptr && result_.r3 > 0) {
            obj.Set("r4", Napi::Buffer<uint8_t>::Copy(env, (uint8_t*)result_.r4, result_.r3));
        } else {
            obj.Set("r4", env.Null());
        }
        Callback().Call({env.Null(), obj});
    }
private:
    int64_t poolId_;
    CreateConnection_return result_;
};

Napi::Value CreateConnectionWrapper(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    int64_t pid = info[0].As<Napi::Number>().Int64Value();
    Napi::Function cb = info[1].As<Napi::Function>();
    CreateConnectionWorker* worker = new CreateConnectionWorker(cb, pid);
    worker->Queue();
    return env.Undefined();
}

//
// Worker 4: CloseConnection asynchronously
//
class CloseConnectionWorker : public Napi::AsyncWorker {
public:
    CloseConnectionWorker(Napi::Function& callback, int64_t poolId, int64_t connId)
        : AsyncWorker(callback), poolId_(poolId), connId_(connId), result_({0, 0, 0, 0, nullptr}) {}

    void Execute() override {
        result_ = CloseConnection(poolId_, connId_);
    }

    void OnOK() override {
        Napi::Env env = Env();
        Napi::Object obj = Napi::Object::New(env);
        obj.Set("r1", Napi::Number::New(env, result_.r1));
        Callback().Call({env.Null(), obj});
    }
private:
    int64_t poolId_, connId_;
    CloseConnection_return result_;
};

Napi::Value CloseConnectionWrapper(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    int64_t pid = info[0].As<Napi::Number>().Int64Value();
    int64_t cid = info[1].As<Napi::Number>().Int64Value();
    Napi::Function cb = info[2].As<Napi::Function>();
    CloseConnectionWorker* worker = new CloseConnectionWorker(cb, pid, cid);
    worker->Queue();
    return env.Undefined();
}

//
// Worker 5: Execute asynchronously
//
class ExecuteWorker : public Napi::AsyncWorker {
public:
    ExecuteWorker(Napi::Function& callback, int64_t poolId, int64_t connId, std::string payload)
        : AsyncWorker(callback), poolId_(poolId), connId_(connId), payload_(payload), result_({0, 0, 0, 0, nullptr}) {}

    void Execute() override {
        GoSlice goPayload = {(void*)payload_.data(), (ptrdiff_t)payload_.length(), (ptrdiff_t)payload_.length()};
        result_ = ::Execute(poolId_, connId_, goPayload);
    }

    void OnOK() override {
        Napi::Env env = Env();
        Napi::Object obj = Napi::Object::New(env);
        obj.Set("r0", Napi::Number::New(env, result_.r0));
        obj.Set("r1", Napi::Number::New(env, result_.r1));
        obj.Set("r2", Napi::Number::New(env, result_.r2));
        obj.Set("r3", Napi::Number::New(env, result_.r3));
        if (result_.r4 != nullptr && result_.r3 > 0) {
            obj.Set("r4", Napi::Buffer<uint8_t>::Copy(env, (uint8_t*)result_.r4, result_.r3));
        } else {
            obj.Set("r4", env.Null());
        }
        Callback().Call({env.Null(), obj});
    }
private:
    int64_t poolId_, connId_;
    std::string payload_;
    Execute_return result_;
};

Napi::Value ExecuteWrapper(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    int64_t pid = info[0].As<Napi::Number>().Int64Value();
    int64_t cid = info[1].As<Napi::Number>().Int64Value();
    
    Napi::Buffer<uint8_t> buffer = info[2].As<Napi::Buffer<uint8_t>>();
    std::string payload(reinterpret_cast<const char*>(buffer.Data()), buffer.Length());
    
    Napi::Function cb = info[3].As<Napi::Function>();
    ExecuteWorker* worker = new ExecuteWorker(cb, pid, cid, payload);
    worker->Queue();
    return env.Undefined();
}

//
// Worker 6: Next asynchronously
//
class NextWorker : public Napi::AsyncWorker {
public:
    NextWorker(Napi::Function& callback, int64_t poolId, int64_t connId, int64_t rowsId, int32_t numRows, int32_t encodeOtp)
        : AsyncWorker(callback), poolId_(poolId), connId_(connId), rowsId_(rowsId), numRows_(numRows), encodeOtp_(encodeOtp), result_({0, 0, 0, 0, nullptr}) {}

    void Execute() override {
        result_ = ::Next(poolId_, connId_, rowsId_, numRows_, encodeOtp_);
    }

    void OnOK() override {
        Napi::Env env = Env();
        Napi::Object obj = Napi::Object::New(env);
        obj.Set("r0", Napi::Number::New(env, result_.r0));
        obj.Set("r1", Napi::Number::New(env, result_.r1));
        obj.Set("r2", Napi::Number::New(env, result_.r2));
        obj.Set("r3", Napi::Number::New(env, result_.r3));
        if (result_.r4 != nullptr && result_.r3 > 0) {
            obj.Set("r4", Napi::Buffer<uint8_t>::Copy(env, (uint8_t*)result_.r4, result_.r3));
        } else {
            obj.Set("r4", env.Null());
        }
        Callback().Call({env.Null(), obj});
    }
private:
    int64_t poolId_, connId_, rowsId_;
    int32_t numRows_, encodeOtp_;
    Next_return result_;
};

Napi::Value NextWrapper(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    int64_t pid = info[0].As<Napi::Number>().Int64Value();
    int64_t cid = info[1].As<Napi::Number>().Int64Value();
    int64_t rid = info[2].As<Napi::Number>().Int64Value();
    int32_t num = info[3].As<Napi::Number>().Int32Value();
    int32_t encode = info[4].As<Napi::Number>().Int32Value();
    Napi::Function cb = info[5].As<Napi::Function>();
    
    NextWorker* worker = new NextWorker(cb, pid, cid, rid, num, encode);
    worker->Queue();
    return env.Undefined();
}

//
// Worker 7: Metadata asynchronously
//
class MetadataWorker : public Napi::AsyncWorker {
public:
    MetadataWorker(Napi::Function& callback, int64_t poolId, int64_t connId, int64_t rowsId)
        : AsyncWorker(callback), poolId_(poolId), connId_(connId), rowsId_(rowsId), result_({0, 0, 0, 0, nullptr}) {}

    void Execute() override {
        result_ = ::Metadata(poolId_, connId_, rowsId_);
    }

    void OnOK() override {
        Napi::Env env = Env();
        Napi::Object obj = Napi::Object::New(env);
        obj.Set("r0", Napi::Number::New(env, result_.r0));
        obj.Set("r1", Napi::Number::New(env, result_.r1));
        obj.Set("r2", Napi::Number::New(env, result_.r2));
        obj.Set("r3", Napi::Number::New(env, result_.r3));
        if (result_.r4 != nullptr && result_.r3 > 0) {
            obj.Set("r4", Napi::Buffer<uint8_t>::Copy(env, (uint8_t*)result_.r4, result_.r3));
        } else {
            obj.Set("r4", env.Null());
        }
        Callback().Call({env.Null(), obj});
    }
private:
    int64_t poolId_, connId_, rowsId_;
    Metadata_return result_;
};

Napi::Value MetadataWrapper(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    int64_t pid = info[0].As<Napi::Number>().Int64Value();
    int64_t cid = info[1].As<Napi::Number>().Int64Value();
    int64_t rid = info[2].As<Napi::Number>().Int64Value();
    Napi::Function cb = info[3].As<Napi::Function>();
    
    MetadataWorker* worker = new MetadataWorker(cb, pid, cid, rid);
    worker->Queue();
    return env.Undefined();
}

// Memory Release (Synchronous as it is just freeing RAM via GC)
Napi::Value NativeRelease(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (info.Length() < 1 || !info[0].IsNumber()) return env.Null();
    int64_t pid = info[0].As<Napi::Number>().Int64Value();
    Release(pid);
    return env.Undefined();
}

// CloseRows dummy/missing implementation for POC length if needed, or we just rely on GC.
Napi::Value CloseRowsWrapper(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (info.Length() < 3) return env.Null();
    int64_t pid = info[0].As<Napi::Number>().Int64Value();
    int64_t cid = info[1].As<Napi::Number>().Int64Value();
    int64_t rid = info[2].As<Napi::Number>().Int64Value();
    
    // N-API sync close implementation
    CloseRows(pid, cid, rid);
    
    // invokeAsync appends a callback at the end of properties
    if (info.Length() >= 4 && info[3].IsFunction()) {
        Napi::Object obj = Napi::Object::New(env);
        obj.Set("r1", Napi::Number::New(env, 0));
        Napi::Function cb = info[3].As<Napi::Function>();
        cb.Call({env.Null(), obj}); // Mock empty GoReturnTuple callback
    }
    return env.Undefined();
}

Napi::Object Init(Napi::Env env, Napi::Object exports) {
    exports.Set("CreatePool", Napi::Function::New(env, CreatePoolWrapper));
    exports.Set("ClosePool", Napi::Function::New(env, ClosePoolWrapper));
    exports.Set("CreateConnection", Napi::Function::New(env, CreateConnectionWrapper));
    exports.Set("CloseConnection", Napi::Function::New(env, CloseConnectionWrapper));
    exports.Set("Execute", Napi::Function::New(env, ExecuteWrapper));
    exports.Set("Next", Napi::Function::New(env, NextWrapper));
    
    exports.Set("CloseRows", Napi::Function::New(env, CloseRowsWrapper));
    exports.Set("Release", Napi::Function::New(env, NativeRelease));
    
    exports.Set("Metadata", Napi::Function::New(env, MetadataWrapper));
    return exports;
}

NODE_API_MODULE(spanner_napi, Init)
