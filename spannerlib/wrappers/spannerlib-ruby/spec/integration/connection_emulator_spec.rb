# frozen_string_literal: true

require "spec_helper"
require 'google/cloud/spanner'

RSpec.describe "Connection APIs against Spanner emulator", :integration do
  before(:all) do
    @emulator_host = ENV["SPANNER_EMULATOR_HOST"]
    unless @emulator_host && !@emulator_host.empty?
      skip "SPANNER_EMULATOR_HOST not set; skipping emulator integration tests"
    end
    
    require "spannerlib/pool"
    @dsn = "projects/your-project-id/instances/test-instance/databases/test-database?autoConfigEmulator=true"
  end

  it "test creation of connection pool" do
    pool = Pool.create_pool(@dsn)
    conn = pool.create_connection
    
    expect(conn).not_to be_nil
    expect(conn.conn_id).to be > 0
    
    conn.close
    pool.close
  end

  it "test two connections from same pool" do
    pool = Pool.create_pool(@dsn)
    conn1 = pool.create_connection
    conn2 = pool.create_connection  
    
    expect(conn1.conn_id).not_to eq(conn2.conn_id)  
    expect(conn1.conn_id).to be > 0
    expect(conn2.conn_id).to be > 0
    
    conn1.close
    conn2.close
    pool.close
  end
  
  it "test write mutations with read-write transactions" do
    pool = Pool.create_pool(@dsn)
    conn = pool.create_connection

    ddl_batch_req = Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest.new(
      statements: [
        Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest::Statement.new(
          sql: "DROP TABLE IF EXISTS test_table"
        ),
        Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest::Statement.new(
          sql: "CREATE TABLE test_table (id INT64 NOT NULL, name STRING(100)) PRIMARY KEY(id)"
        )
      ]
    )
    conn.execute_batch(ddl_batch_req)

    conn.begin_transaction
    
    insert_data_req = Google::Spanner::V1::BatchWriteRequest::MutationGroup.new(
      mutations: [
        Google::Spanner::V1::Mutation.new(
          insert: Google::Spanner::V1::Mutation::Write.new(
            table: "test_table",
            columns: ["id", "name"],
            values: [
              Google::Protobuf::ListValue.new(values: [Google::Protobuf::Value.new(string_value: "1"), Google::Protobuf::Value.new(string_value: "Alice")]),
              Google::Protobuf::ListValue.new(values: [Google::Protobuf::Value.new(string_value: "2"), Google::Protobuf::Value.new(string_value: "Bob")])
            ]
          )
        )
      ]
    )
    conn.write_mutations(insert_data_req)
    
    conn.commit
    select_req = Google::Cloud::Spanner::V1::ExecuteSqlRequest.new(
      sql: "SELECT id, name FROM test_table ORDER BY id"
    )
    rows_id = conn.execute(select_req)
    expect(rows_id).to be > 0

    all_rows = []
    loop do
      row_bytes = conn.next_rows(rows_id, 1)
      break if row_bytes.nil? || row_bytes.empty?
      
      all_rows << Google::Protobuf::ListValue.decode(row_bytes)
    end
    
    conn.close_rows(rows_id)

    expect(all_rows.length).to eq(2)
    
    expect(all_rows[0].values[0].string_value).to eq("1")
    expect(all_rows[0].values[1].string_value).to eq("Alice")
    
    expect(all_rows[1].values[0].string_value).to eq("2")
    expect(all_rows[1].values[1].string_value).to eq("Bob")

    conn.close
    pool.close
  end

  it "test write mutations without transactions" do
    pool = Pool.create_pool(@dsn)
    conn = pool.create_connection

    ddl_batch_req = Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest.new(
      statements: [
        Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest::Statement.new(
          sql: "DROP TABLE IF EXISTS test_table_1"
        ),
        Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest::Statement.new(
          sql: "CREATE TABLE test_table_1 (id INT64 NOT NULL, name STRING(100)) PRIMARY KEY(id)"
        )
      ]
    )
    conn.execute_batch(ddl_batch_req)

    insert_data_req = Google::Spanner::V1::BatchWriteRequest::MutationGroup.new(
      mutations: [
        Google::Spanner::V1::Mutation.new(
          insert: Google::Spanner::V1::Mutation::Write.new(
            table: "test_table_1",
            columns: ["id", "name"],
            values: [
              Google::Protobuf::ListValue.new(values: [Google::Protobuf::Value.new(string_value: "1"), Google::Protobuf::Value.new(string_value: "Charlie")]),
              Google::Protobuf::ListValue.new(values: [Google::Protobuf::Value.new(string_value: "2"), Google::Protobuf::Value.new(string_value: "David")])
            ]
          )
        )
      ]
    )
    conn.write_mutations(insert_data_req)
    
    select_req = Google::Cloud::Spanner::V1::ExecuteSqlRequest.new(
      sql: "SELECT id, name FROM test_table_1 ORDER BY id"
    )
    rows_id = conn.execute(select_req)
    expect(rows_id).to be > 0

    all_rows = []
    loop do
      row_bytes = conn.next_rows(rows_id, 1)
      break if row_bytes.nil? || row_bytes.empty?
      all_rows << Google::Protobuf::ListValue.decode(row_bytes)
    end
    conn.close_rows(rows_id)
    
    expect(all_rows.length).to eq(2)
    expect(all_rows[0].values[1].string_value).to eq("Charlie")
    expect(all_rows[1].values[1].string_value).to eq("David")
    
    conn.close
    pool.close
  end

  it "raises an error when writing mutations in a read-only transaction" do
    
    pool = Pool.create_pool(@dsn)
    conn = pool.create_connection

    ddl_batch_req = Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest.new(
      statements: [
        Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest::Statement.new(
          sql: "DROP TABLE IF EXISTS test_table_2"
        ),
        Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest::Statement.new(
          sql: "CREATE TABLE test_table_2 (id INT64 NOT NULL, name STRING(100)) PRIMARY KEY(id)"
        )
      ]
    )
    conn.execute_batch(ddl_batch_req)

    transaction_options = Google::Spanner::V1::TransactionOptions.new(
      read_only: Google::Spanner::V1::TransactionOptions::ReadOnly.new(
        strong: true
      )
    )
    conn.begin_transaction(transaction_options)

    insert_data_req = Google::Spanner::V1::BatchWriteRequest::MutationGroup.new(
        mutations: [
          Google::Spanner::V1::Mutation.new(
            insert: Google::Spanner::V1::Mutation::Write.new(
              table: "test_table_2",
              columns: ["id", "name"],
              values: [
                Google::Protobuf::ListValue.new(values: [Google::Protobuf::Value.new(string_value: "1"), Google::Protobuf::Value.new(string_value: "Charlie")]),
                Google::Protobuf::ListValue.new(values: [Google::Protobuf::Value.new(string_value: "2"), Google::Protobuf::Value.new(string_value: "David")])
              ]
            )
          )
        ]
      )

    expect {
      conn.write_mutations(insert_data_req)
    }.to raise_error(RuntimeError, /read-only transactions cannot write/)

    conn.rollback
    conn.close
    pool.close
  end
end