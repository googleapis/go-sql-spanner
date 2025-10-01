require_relative 'ffi'
require_relative 'connection'

class Pool
  attr_reader :id

  def initialize(id)
    @id = id
    @closed = false
  end

  # Create a new Pool given a DSN string. Raises SpannerLibException on failure.
  def self.create_pool(dsn)
    begin
      pool_id = SpannerLib.create_pool(dsn)
    rescue => e
      raise SpannerLibException.new(e.message)
    end

    if pool_id.nil? || pool_id <= 0
      raise SpannerLibException.new('failed to create pool')
    end

    Pool.new(pool_id)
  end

  # Close this pool and free native resources.
  def close
    return if @closed
    SpannerLib.close_pool(@id)
    @closed = true
  end

  # Create a new Connection associated with this Pool.
  def create_connection
    raise SpannerLibException.new('pool closed') if @closed
    begin
      conn_id = SpannerLib.create_connection(@id)
    rescue => e
      raise SpannerLibException.new(e.message)
    end

    if conn_id.nil? || conn_id <= 0
      raise SpannerLibException.new('failed to create connection')
    end

    Connection.new(@id, conn_id)
  end
end



