# frozen_string_literal: true

module SpannerLib
  class Rows
    include Enumerable

    attr_reader :id, :connection

    def initialize(connection, rows_id)
      @connection = connection
      @id = rows_id
      @closed = false
    end

    def each
      return enum_for(:each) unless block_given?

      while (row = self.next)
        yield row
      end
    ensure
      close
    end

    def next
      return nil if @closed

      row_data = SpannerLib.next(connection.pool_id, connection.conn_id, id, 1, 0)

      if row_data.nil? || row_data.empty? || (row_data.respond_to?(:values) && row_data.values.empty?)
        close
        return nil
      end

      row_data
    end

    def metadata
      SpannerLib.metadata(connection.pool_id, connection.conn_id, id)
    end

    def result_set_stats
      SpannerLib.result_set_stats(connection.pool_id, connection.conn_id, id)
    end

    def close
      return if @closed

      SpannerLib.close_rows(connection.pool_id, connection.conn_id, id)
      @closed = true
    end
  end
end
