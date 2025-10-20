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
require "spannerlib/pool"
require "spannerlib/ffi"
require "spannerlib/exceptions"

RSpec.describe Pool do
  let(:dsn) { "localhost:1234/projects/p/instances/i/databases/d?usePlainText=true" }

  describe ".create_pool" do
    it "creates a pool and returns an object with id > 0" do
      allow(SpannerLib).to receive(:create_pool).with(dsn).and_return(42)

      pool = described_class.create_pool(dsn)

      expect(pool).to be_a(described_class)
      expect(pool.id).to be > 0
    end

    it "raises a SpannerLibException when create_session/create_pool fails" do
      allow(SpannerLib).to receive(:create_pool).with(dsn).and_raise(StandardError.new("Not allowed"))

      expect { described_class.create_pool(dsn) }.to raise_error(SpannerLibException)
    end

    it "raises when create_pool returns nil" do
      allow(SpannerLib).to receive(:create_pool).with(dsn).and_return(nil)

      expect { described_class.create_pool(dsn) }.to raise_error(SpannerLibException)
    end

    it "raises when create_pool returns a non-positive id" do
      allow(SpannerLib).to receive(:create_pool).with(dsn).and_return(0)

      expect { described_class.create_pool(dsn) }.to raise_error(SpannerLibException)
    end
  end
end
