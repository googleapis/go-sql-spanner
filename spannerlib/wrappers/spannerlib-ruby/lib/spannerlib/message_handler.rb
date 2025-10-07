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

# lib/spannerlib/message_handler.rb

require "spannerlib/exceptions"

module SpannerLib
  class MessageHandler
    def initialize(message)
      @message = message
    end

    def object_id
      throw_if_error!
      @message[:objectId]
    end

    # Returns the data payload from the message.
    # If a proto_klass is provided, it decodes the bytes into a Protobuf object.
    # Otherwise, it returns the raw bytes as a string.
    def data(proto_klass: nil)
      throw_if_error!

      len = @message[:length]
      ptr = @message[:pointer]

      return (proto_klass ? proto_klass.new : "") unless len.positive? && !ptr.null?

      bytes = ptr.read_string(len)

      proto_klass ? proto_klass.decode(bytes) : bytes
    end

    def throw_if_error!
      code = @message[:code]
      return if code.zero?

      error_msg = SpannerLib.read_error_message(@message)
      raise SpannerLibException, "Call failed with code #{code}: #{error_msg}"
    end
  end
end
