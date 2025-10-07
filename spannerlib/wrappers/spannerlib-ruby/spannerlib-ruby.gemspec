# frozen_string_literal: true

require_relative "lib/spannerlib/ruby/version"

Gem::Specification.new do |spec|
  spec.name = "spannerlib-ruby"
  spec.version = Spannerlib::Ruby::VERSION
  spec.authors = ["Google LLC"]
  spec.email = ["cloud-spanner-developers@googlegroups.com"]

  spec.summary = "Ruby wrapper for the Spanner native library"
  spec.description = "Lightweight Ruby FFI bindings for the Spanner native library produced from the Go implementation."
  spec.homepage = "https://github.com/googleapis/go-sql-spanner/tree/main/spannerlib/wrappers/spannerlib-ruby"
  spec.license = "Apache 2.0 License"
  spec.required_ruby_version = ">= 3.1.0"

  spec.metadata["rubygems_mfa_required"] = "true"

  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "ffi"
  spec.add_dependency "google-cloud-spanner-v1", "~> 1.7"
  spec.add_dependency "google-protobuf", "~> 3.19"
  spec.add_dependency "grpc", "~> 1.60"
end
