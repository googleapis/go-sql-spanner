# frozen_string_literal: true

# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require_relative "statement_result"
require "google/rpc/error_details_pb"
require "google/spanner/v1/spanner_pb"
require "google/spanner/v1/spanner_services_pb"
require "google/cloud/spanner/v1/spanner"

require "grpc"
require "gapic/grpc/service_stub"
require "securerandom"

# Mock implementation of Spanner
class SpannerMockServer < Google::Cloud::Spanner::V1::Spanner::Service
  attr_reader :requests

  def initialize
    super
    @statement_results = {}
    @sessions = {}
    @transactions = {}
    @aborted_transactions = {}
    @requests = []
    @errors = {}
    @session_counter = 0

    @mock_file_path = ENV.fetch("MOCK_MSG_FILE", nil)
    @req_file_path = ENV.fetch("MOCK_REQ_FILE", nil)

    add_default_results
  end

  def log_request(request)
    @requests << request
    return unless @req_file_path

    # Guard clause: Ensure we only attempt to encode Protobuf objects.
    return unless request.class.respond_to?(:encode)

    begin
      data = {
        class: request.class.name,
        payload: request.class.encode(request)
      }
      File.open(@req_file_path, "ab") do |f|
        Marshal.dump(data, f)
      end
    rescue StandardError => e
      warn "Failed to log request: #{e.message}"
    end
  end

  def put_statement_result(sql, result)
    @statement_results[sql] = result
  end

  def push_error(sql_or_method, error)
    @errors[sql_or_method] = [] unless @errors[sql_or_method]
    @errors[sql_or_method].push error
  end

  def create_session(request, _unused_call)
    log_request(request)
    do_create_session(request.database, request.session)
  end

  def batch_create_sessions(request, _unused_call)
    log_request(request)
    num_created = 0
    response = Google::Cloud::Spanner::V1::BatchCreateSessionsResponse.new
    while num_created < request.session_count
      response.session << do_create_session(request.database, request.session_template)
      num_created += 1
    end
    response
  end

  def get_session(request, _unused_call)
    log_request(request)
    @sessions[request.name]
  end

  def list_sessions(request, _unused_call)
    log_request(request)
    response = Google::Cloud::Spanner::V1::ListSessionsResponse.new
    @sessions.each_value do |s|
      response.sessions << s
    end
    response
  end

  def delete_session(request, _unused_call)
    log_request(request)
    @sessions.delete request.name
    Google::Protobuf::Empty.new
  end

  def execute_sql(request, _unused_call)
    log_request(request)
    do_execute_sql request, false
  end

  def execute_streaming_sql(request, _unused_call)
    log_request(request)
    do_execute_sql request, true
  end

  # @private
  def do_execute_sql(request, streaming)
    validate_session request.session
    created_transaction = do_create_transaction request.session if request.transaction&.begin
    transaction_id = created_transaction&.id || request.transaction&.id
    validate_transaction request.session, transaction_id if transaction_id && transaction_id != ""

    raise @errors[request.sql].pop if @errors[request.sql] && !@errors[request.sql].empty?

    results_or_single = get_statement_result(request.sql)
    results = results_or_single.is_a?(Array) ? results_or_single : [results_or_single]

    # Handle streaming vs non-streaming response
    # Streaming: return an enumerator that yields all result sets
    if streaming
      Enumerator.new do |yielder|
        results.each do |entry|
          result = entry.clone
          raise_if_error!(result)

          result.each(created_transaction) do |partial|
            yielder << partial
          end
        end
      end
    else
      # Non-streaming: only return the first result set
      entry = results.first
      result = entry.clone
      raise_if_error!(result)
      result.result(created_transaction)
    end
  end

  def execute_batch_dml(request, _unused_call)
    log_request(request)
    validate_session request.session
    created_transaction = do_create_transaction request.session if request.transaction&.begin
    transaction_id = created_transaction&.id || request.transaction&.id
    validate_transaction request.session, transaction_id if transaction_id && transaction_id != ""

    status = Google::Rpc::Status.new
    response = Google::Cloud::Spanner::V1::ExecuteBatchDmlResponse.new
    first = true
    request.statements.each do |stmt|
      result = get_statement_result(stmt.sql).clone
      if result.result_type == StatementResult::EXCEPTION
        err_proto = result.result
        if err_proto.is_a?(Google::Rpc::Status)
          status.code = err_proto.code
          status.message = err_proto.message
        else
          status.code = GRPC::Core::StatusCodes::UNKNOWN
          status.message = err_proto.to_s
        end
        break
      end
      if first
        response.result_sets << result.result(created_transaction)
        first = false
      else
        response.result_sets << result.result
      end
    end
    response.status = status
    response
  end

  def read(request, _unused_call)
    log_request(request)
    raise GRPC::BadStatus.new GRPC::Core::StatusCodes::UNIMPLEMENTED, "Not yet implemented"
  end

  def streaming_read(request, _unused_call)
    log_request(request)
    raise GRPC::BadStatus.new GRPC::Core::StatusCodes::UNIMPLEMENTED, "Not yet implemented"
  end

  def begin_transaction(request, _unused_call)
    log_request(request)
    raise @errors[__method__.to_s].pop if @errors[__method__.to_s] && !@errors[__method__.to_s].empty?

    validate_session request.session
    do_create_transaction request.session
  end

  def commit(request, _unused_call)
    log_request(request)
    validate_session request.session
    validate_transaction request.session, request.transaction_id
    Google::Cloud::Spanner::V1::CommitResponse.new commit_timestamp: Google::Protobuf::Timestamp.new(seconds: Time.now.to_i)
  end

  def rollback(request, _unused_call)
    log_request(request)
    validate_session request.session
    name = "#{request.session}/transactions/#{request.transaction_id}"
    @transactions.delete name
    Google::Protobuf::Empty.new
  end

  def partition_query(request, _unused_call)
    log_request(request)
    raise GRPC::BadStatus.new GRPC::Core::StatusCodes::UNIMPLEMENTED, "Not yet implemented"
  end

  def partition_read(request, _unused_call)
    log_request(request)
    raise GRPC::BadStatus.new GRPC::Core::StatusCodes::UNIMPLEMENTED, "Not yet implemented"
  end

  def get_database(request, _unused_call)
    log_request(request)
    raise GRPC::BadStatus.new GRPC::Core::StatusCodes::UNIMPLEMENTED, "Not yet implemented"
  end

  def abort_transaction(session, id)
    return if session.nil? || id.nil?

    name = "#{session}/transactions/#{id}"
    @aborted_transactions[name] = true
  end

  def abort_next_transaction
    @abort_next_transaction = true
  end

  def get_statement_result(sql)
    load_dynamic_mocks!

    unless @statement_results.key? sql
      @statement_results.each do |key, value|
        return value if key.end_with?("%") && sql.start_with?(key.chop)
      end
      available_keys = @statement_results.keys.join(", ")
      raise GRPC::BadStatus.new(
        GRPC::Core::StatusCodes::INVALID_ARGUMENT,
        "No result registered for '#{sql}'. Available: [#{available_keys}]"
      )
    end
    @statement_results[sql]
  end

  def load_dynamic_mocks!
    return unless @mock_file_path && File.exist?(@mock_file_path)

    begin
      content = File.binread(@mock_file_path)
      return if content.empty?

      new_mocks = Marshal.load(content)
      @statement_results.merge!(new_mocks)
    rescue StandardError => e
      warn "Failed to load mocks: #{e.message}"
    end
  end

  def add_default_results
    put_statement_result "SELECT 1", StatementResult.create_select1_result
    put_statement_result "INSERT INTO test_table (id, name) VALUES ('1', 'Alice')", StatementResult.create_update_count_result(1)

    dialect_sql = "select option_value from information_schema.database_options where option_name='database_dialect'"
    put_statement_result dialect_sql, StatementResult.create_dialect_result
  end

  def delete_all_sessions
    @sessions.clear
  end

  private

  def validate_session(session)
    return if @sessions.key? session

    resource_info = Google::Rpc::ResourceInfo.new(
      resource_type: "type.googleapis.com/google.spanner.v1.Session",
      resource_name: session
    )
    raise GRPC::BadStatus.new(
      GRPC::Core::StatusCodes::NOT_FOUND,
      "Session not found: Session with id #{session} not found",
      { "google.rpc.resourceinfo-bin": Google::Rpc::ResourceInfo.encode(resource_info) }
    )
  end

  def do_create_session(database, session_template = nil)
    @session_counter += 1
    name = "#{database}/sessions/mock-session-#{@session_counter}"
    session = Google::Cloud::Spanner::V1::Session.new(name: name)

    session.multiplexed = session_template.multiplexed if session_template

    @sessions[name] = session
    session
  end

  def validate_transaction(session, transaction)
    name = "#{session}/transactions/#{transaction}"
    unless @transactions.key? name
      raise GRPC::BadStatus.new(
        GRPC::Core::StatusCodes::NOT_FOUND,
        "Transaction not found: Transaction with id #{transaction} not found"
      )
    end
    return unless @aborted_transactions.key?(name)

    retry_info = Google::Rpc::RetryInfo.new(retry_delay: Google::Protobuf::Duration.new(seconds: 0, nanos: 1))
    raise GRPC::BadStatus.new(
      GRPC::Core::StatusCodes::ABORTED,
      "Transaction aborted",
      { "google.rpc.retryinfo-bin": Google::Rpc::RetryInfo.encode(retry_info) }
    )
  end

  def do_create_transaction(session)
    id = SecureRandom.uuid
    name = "#{session}/transactions/#{id}"
    transaction = Google::Cloud::Spanner::V1::Transaction.new id: id
    @transactions[name] = transaction
    if @abort_next_transaction
      abort_transaction session, id
      @abort_next_transaction = false
    end
    transaction
  end

  def raise_if_error!(result)
    return unless result.result_type == StatementResult::EXCEPTION

    err_proto = result.result

    raise GRPC::BadStatus.new(err_proto.code, err_proto.message) if err_proto.is_a?(Google::Rpc::Status)

    raise err_proto
  end
end
