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

# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Connection APIs against Spanner emulator", :integration do
  before(:all) do
    @emulator_host = ENV.fetch("SPANNER_EMULATOR_HOST", nil)
    skip "SPANNER_EMULATOR_HOST not set; skipping emulator integration tests" unless @emulator_host && !@emulator_host.empty?

    begin
      require "spannerlib/pool"
    rescue LoadError, StandardError => e
      skip "Could not load native spanner library; skipping emulator integration tests: #{e.class}: #{e.message}"
    end
    @dsn = "projects/your-project-id/instances/test-instance/databases/test-database?autoConfigEmulator=true"
  end

  it "creates a pool and a connection against the emulator" do
    pool = Pool.create_pool(@dsn)
    expect(pool.id).to be > 0

    conn = pool.create_connection
    expect(conn).to respond_to(:pool_id)
    expect(conn).to respond_to(:conn_id)

    expect { pool.close }.not_to raise_error
  end
end
