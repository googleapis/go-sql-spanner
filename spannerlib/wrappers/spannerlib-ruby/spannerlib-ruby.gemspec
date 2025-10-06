# frozen_string_literal: true

require_relative "lib/spannerlib/ruby/version"

Gem::Specification.new do |spec|
  spec.name = "spannerlib-ruby"
  spec.version = Spannerlib::Ruby::VERSION
  spec.authors = ["Spannerlib Contributors"]
  spec.email = ["spannerlib@example.com"]

  spec.summary = "Ruby wrapper for the Spanner native library"
  spec.description = "Lightweight Ruby FFI bindings for the Spanner native library produced from the Go implementation."
  # Use an example homepage for local builds; replace with your project's URL.
  spec.homepage = "https://example.com/spannerlib-ruby"
  spec.license = "Apache 2.0 License"
  spec.required_ruby_version = ">= 3.1.0"

  spec.metadata["allowed_push_host"] = "TODO: Set to your gem server 'https://example.com'"

  spec.metadata["homepage_uri"] = spec.homepage || "https://example.com"
  spec.metadata["source_code_uri"] = "https://example.com/source"
  spec.metadata["changelog_uri"] = "https://example.com/CHANGELOG.md"
  spec.metadata["rubygems_mfa_required"] = "true"

  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "ffi"
  spec.add_dependency "google-cloud-spanner-v1", "~> 1.7"
  spec.add_dependency "google-protobuf", "~> 3.19"
  spec.add_dependency "grpc", "~> 1.60"
end
