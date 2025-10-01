# frozen_string_literal: true

# rubocop:disable Metrics/ModuleLength

require "rubygems"
require "bundler/setup"

require "google/protobuf"
require "google/rpc/status_pb"

require "ffi"

module SpannerLib
  extend FFI::Library

  ffi_lib File.expand_path("../../shared/spannerlib.so", __dir__)

  class GoString < FFI::Struct
    layout :p,   :pointer,
           :len, :long
  end

  # GoBytes is the Ruby representation of a Go byte slice
  class GoBytes < FFI::Struct
    layout :p,   :pointer,
           :len, :long,
           :cap, :long
  end

  # Message is the common return type for all native functions.
  class Message < FFI::Struct
    layout :pinner,   :long_long,
           :code,     :int,
           :objectId, :long_long,
           :length,   :int,
           :pointer,  :pointer
  end

  # --- Native Function Signatures ---
  attach_function :CreatePool, [GoString.by_value], Message.by_value
  attach_function :ClosePool, [:int64], Message.by_value
  attach_function :CreateConnection, [:int64], Message.by_value
  attach_function :CloseConnection, %i[int64 int64], Message.by_value
  attach_function :WriteMutations, [:int64, :int64, GoBytes.by_value], Message.by_value
  attach_function :BeginTransaction, [:int64, :int64, GoBytes.by_value], Message.by_value
  attach_function :Commit, %i[int64 int64], Message.by_value
  attach_function :Rollback, %i[int64 int64], Message.by_value
  attach_function :Execute, [:int64, :int64, GoBytes.by_value], Message.by_value
  attach_function :ExecuteBatch, [:int64, :int64, GoBytes.by_value], Message.by_value
  attach_function :Metadata, %i[int64 int64 int64], Message.by_value
  attach_function :Next, %i[int64 int64 int64 int32 int32], Message.by_value
  attach_function :ResultSetStats, %i[int64 int64 int64], Message.by_value
  attach_function :CloseRows, %i[int64 int64 int64], Message.by_value
  attach_function :Release, [:int64], :void

  # --- Ruby-friendly Wrappers ---

  def self.create_pool(dsn)
    dsn_str = dsn.to_s.dup
    dsn_ptr = FFI::MemoryPointer.from_string(dsn_str)

    go_dsn = GoString.new
    go_dsn[:p] = dsn_ptr
    go_dsn[:len] = dsn_str.bytesize

    message = CreatePool(go_dsn)
    handle_object_id_response(message, "CreatePool")
  end

  def self.close_pool(pool_id)
    message = ClosePool(pool_id)
    handle_status_response(message, "ClosePool")
    nil
  end

  def self.create_connection(pool_id)
    message = CreateConnection(pool_id)
    handle_object_id_response(message, "CreateConnection")
  end

  def self.close_connection(pool_id, conn_id)
    message = CloseConnection(pool_id, conn_id)
    handle_status_response(message, "CloseConnection")
    nil
  end

  def self.release(pinner)
    Release(pinner)
  end

  def self.with_gobytes(bytes)
    bytes ||= ""
    len = bytes.bytesize
    ptr = FFI::MemoryPointer.new(len)
    ptr.write_bytes(bytes, 0, len) if len.positive?

    go_bytes = GoBytes.new
    go_bytes[:p] = ptr
    go_bytes[:len] = len
    go_bytes[:cap] = len

    yield(go_bytes)
  end

  def self.ensure_release(message)
    pinner = message[:pinner]
    begin
      yield
    ensure
      release(pinner) if pinner != 0
    end
  end

  def self.handle_object_id_response(message, func_name)
    ensure_release(message) do
      if message[:code] != 0
        error_msg = read_error_message(message)
        raise "#{func_name} failed with code #{message[:code]}: #{error_msg}"
      end
      message[:objectId]
    end
  end

  def self.handle_status_response(message, func_name)
    ensure_release(message) do
      if message[:code] != 0
        error_msg = read_error_message(message)
        raise "#{func_name} failed with code #{message[:code]}: #{error_msg}"
      end
    end
  end

  # rubocop:disable Metrics/MethodLength
  def self.handle_data_response(message, func_name)
    ensure_release(message) do
      if message[:code] != 0
        error_msg = read_error_message(message)
        raise "#{func_name} failed with code #{message[:code]}: #{error_msg}"
      end

      len = message[:length]
      ptr = message[:pointer]

      if len.positive? && !ptr.null?
        ptr.read_bytes(len)
      else
        ""
      end
    end
  end
  # rubocop:enable Metrics/MethodLength

  # rubocop:disable Metrics/MethodLength
  def self.read_error_message(message)
    len = message[:length]
    ptr = message[:pointer]
    if len.positive? && !ptr.null?
      raw_bytes = ptr.read_bytes(len)
      begin
        status_proto = ::Google::Rpc::Status.decode(raw_bytes)
        "Status Proto { code: #{status_proto.code}, message: '#{status_proto.message}' }"
      rescue StandardError => e
        clean_string = raw_bytes.encode("UTF-8", invalid: :replace, undef: :replace, replace: "?").strip
        "Failed to decode Status proto (code #{message[:code]}): #{e.class}: #{e.message} | Raw: #{clean_string}"
      end
    else
      "No error message provided"
    end
  end
  # rubocop:enable Metrics/MethodLength

  def self.write_mutations(pool_id, conn_id, proto_bytes)
    with_gobytes(proto_bytes) do |gobytes|
      message = WriteMutations(pool_id, conn_id, gobytes)
      handle_data_response(message, "WriteMutations")
    end
  end

  def self.begin_transaction(pool_id, conn_id, proto_bytes)
    with_gobytes(proto_bytes) do |gobytes|
      message = BeginTransaction(pool_id, conn_id, gobytes)
      handle_data_response(message, "BeginTransaction")
    end
  end

  def self.commit(pool_id, conn_id)
    message = Commit(pool_id, conn_id)
    handle_data_response(message, "Commit")
  end

  def self.rollback(pool_id, conn_id)
    message = Rollback(pool_id, conn_id)
    handle_status_response(message, "Rollback")
    nil
  end

  def self.execute(pool_id, conn_id, proto_bytes)
    with_gobytes(proto_bytes) do |gobytes|
      message = Execute(pool_id, conn_id, gobytes)
      handle_object_id_response(message, "Execute")
    end
  end

  def self.execute_batch(pool_id, conn_id, proto_bytes)
    with_gobytes(proto_bytes) do |gobytes|
      message = ExecuteBatch(pool_id, conn_id, gobytes)
      handle_data_response(message, "ExecuteBatch")
    end
  end

  def self.metadata(pool_id, conn_id, rows_id)
    message = Metadata(pool_id, conn_id, rows_id)
    handle_data_response(message, "Metadata")
  end

  def self.next(pool_id, conn_id, rows_id, max_rows, fetch_size)
    message = Next(pool_id, conn_id, rows_id, max_rows, fetch_size)
    handle_data_response(message, "Next")
  end

  def self.result_set_stats(pool_id, conn_id, rows_id)
    message = ResultSetStats(pool_id, conn_id, rows_id)
    handle_data_response(message, "ResultSetStats")
  end

  def self.close_rows(pool_id, conn_id, rows_id)
    message = CloseRows(pool_id, conn_id, rows_id)
    handle_status_response(message, "CloseRows")
    nil
  end
end

# rubocop:enable Metrics/ModuleLength
