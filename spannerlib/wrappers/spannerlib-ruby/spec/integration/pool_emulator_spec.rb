# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Pool against Spanner emulator", :integration do
  before(:all) do
    @emulator_host = ENV["SPANNER_EMULATOR_HOST"]
    unless @emulator_host && !@emulator_host.empty?
      skip "SPANNER_EMULATOR_HOST not set; skipping emulator integration tests"
    end

    begin
      require "spannerlib/pool"
    rescue LoadError, StandardError => e
      skip "Could not load native spanner library; skipping emulator integration tests: #{e.class}: #{e.message}"
    end
    @dsn = "projects/your-project-id/instances/test-instance/databases/test-database?autoConfigEmulator=true"
  end

  it "creates a pool and a connection against the emulator" do
    pool = Pool.create_pool(@dsn)
    expect(pool).to be_a(Pool)
    expect(pool.id).to be > 0

    conn = pool.create_connection
    expect(conn).to respond_to(:pool_id)
    expect(conn).to respond_to(:conn_id)

    expect { pool.close }.not_to raise_error
  end

end
