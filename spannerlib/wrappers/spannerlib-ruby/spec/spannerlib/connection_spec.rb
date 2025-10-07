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
# See the License for the specific language gove

# frozen_string_literal: true

require "spec_helper"
require "spannerlib/pool"
require "spannerlib/ffi"
require "spannerlib/exceptions"

RSpec.describe Connection do
  let(:dsn) { "localhost:1234/projects/p/instances/i/databases/d?usePlainText=true" }
  let(:pool) { Pool.create_pool(dsn) }

  before do
    allow(SpannerLib).to receive(:create_pool).and_return(1)
  end

  describe "creation" do
    it "is created by a Pool" do
      allow(SpannerLib).to receive(:create_connection).with(1).and_return(2)
      
      # The object under test is the one returned by `pool.create_connection`
      conn = pool.create_connection

      expect(conn).to be_a(Connection)
      expect(conn.conn_id).to eq(2)
      expect(conn.pool_id).to eq(1)
    end

    it "raises a SpannerLibException when the FFI call fails" do
      allow(SpannerLib).to receive(:create_connection).with(1).and_raise(StandardError.new("boom"))

      expect { pool.create_connection }.to raise_error(SpannerLibException)
    end

    it "raises when the FFI call returns a non-positive id" do
      allow(SpannerLib).to receive(:create_connection).with(1).and_return(0)

      expect { pool.create_connection }.to raise_error(SpannerLibException)
    end
  end
end
