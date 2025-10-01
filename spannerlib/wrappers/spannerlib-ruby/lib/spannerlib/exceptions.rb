# frozen_string_literal: true

class SpannerLibException < StandardError
  attr_reader :status

  def initialize(msg = nil, status = nil)
    super(msg)
    @status = status
  end
end
