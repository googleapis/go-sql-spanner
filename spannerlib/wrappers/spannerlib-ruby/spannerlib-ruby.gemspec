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

  # Include both git-tracked files (for local development) and any built native libraries
  # that exist on disk (for CI). We do this so:
  #  - During local development, spec.files is driven by `git ls-files` (keeps the gem manifest clean).
  #  - In CI we build native shared libraries into lib/spannerlib/<platform>/ and those files are
  #    not checked into git; we therefore also glob lib/spannerlib/** to pick up the CI-built binaries
  #    so the gem produced in CI actually contains the native libraries.
  #  - We explicitly filter out common build artifacts and non-distributable files to avoid accidentally
  #    packaging object files, headers, or temporary files.
  # This allows us to publish a single multi-platform gem that contains prebuilt shared libraries

  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    files = []
    # prefer git-tracked files when available (local dev), but also pick up built files present on disk (CI)
    files += `git ls-files -z`.split("\x0") if system("git rev-parse --is-inside-work-tree > /dev/null 2>&1")

    # include any built native libs (CI places them under lib/spannerlib/)
    files += Dir.glob("lib/spannerlib/**/*").select { |f| File.file?(f) }

    # dedupe and reject unwanted entries
    files.map! { |f| f.sub(%r{\A\./}, "") }.uniq!
    files.reject do |f|
      f.match(%r{^(pkg|Gemfile\.lock|.*\.gem|Rakefile|spec/|.*\.o|.*\.h)$})
    end
  end

  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "ffi"
  spec.add_dependency "google-cloud-spanner-v1", "~> 1.7"
  spec.add_dependency "google-protobuf", "~> 3.19"
  spec.add_dependency "grpc", "~> 1.60"
end
