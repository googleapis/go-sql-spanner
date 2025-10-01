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

            pool = Pool.create_pool(dsn)

            expect(pool).to be_a(Pool)
            expect(pool.id).to be > 0
        end

        it "raises a SpannerLibException when create_session/create_pool fails" do
            allow(SpannerLib).to receive(:create_pool).with(dsn).and_raise(StandardError.new("Not allowed"))

            expect { Pool.create_pool(dsn) }.to raise_error(SpannerLibException)
        end

        it "raises when create_pool returns nil" do
            allow(SpannerLib).to receive(:create_pool).with(dsn).and_return(nil)

            expect { Pool.create_pool(dsn) }.to raise_error(SpannerLibException)
        end

        it "raises when create_pool returns a non-positive id" do
            allow(SpannerLib).to receive(:create_pool).with(dsn).and_return(0)

            expect { Pool.create_pool(dsn) }.to raise_error(SpannerLibException)
        end
    end
end