# frozen_string_literal: true

require "spec_helper"
require "spannerlib/pool"
require "spannerlib/ffi"
require "spannerlib/exceptions" 

RSpec.describe Pool do
    let(:dsn) { "localhost:1234/projects/p/instances/i/databases/d?usePlainText=true" }

    describe "#create_connection" do
        it "creates a Connection associated with this Pool" do
            allow(SpannerLib).to receive(:create_pool).with(dsn).and_return(1)
            expect(SpannerLib).to receive(:create_connection).with(1).and_return(2)

            pool = Pool.create_pool(dsn)
            conn = pool.create_connection

            expect(conn).to be_a(Connection)
        end

        it "raises a SpannerLibException when create_connection fails" do
            allow(SpannerLib).to receive(:create_pool).with(dsn).and_return(1)
            allow(SpannerLib).to receive(:create_connection).with(1).and_raise(StandardError.new("boom"))

            pool = Pool.create_pool(dsn)
            expect { pool.create_connection }.to raise_error(SpannerLibException)
        end

        it "raises when create_connection returns nil or non-positive id" do
            allow(SpannerLib).to receive(:create_pool).with(dsn).and_return(1)
            allow(SpannerLib).to receive(:create_connection).with(1).and_return(nil)

            pool = Pool.create_pool(dsn)
            expect { pool.create_connection }.to raise_error(SpannerLibException)
        end
    end
end