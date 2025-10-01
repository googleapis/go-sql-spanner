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
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.1.0"

  spec.metadata["allowed_push_host"] = "TODO: Set to your gem server 'https://example.com'"

  # Provide minimal valid URIs so `gem build` passes metadata validation. Replace
  # these with the project's real URLs before publishing.
  spec.metadata["homepage_uri"] = spec.homepage || "https://example.com"
  spec.metadata["source_code_uri"] = "https://example.com/source"
  spec.metadata["changelog_uri"] = "https://example.com/CHANGELOG.md"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ test/ spec/ features/ .git .github appveyor Gemfile])
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  # Uncomment to register a new dependency of your gem
  # spec.add_dependency "example-gem", "~> 1.0"
  
  spec.add_dependency "google-cloud-spanner", "~> 2.25"
  spec.add_dependency "google-cloud-spanner-v1", "~> 1.7"

  # For more information and examples about making a new gem, check out our
  # guide at: https://bundler.io/guides/creating_gem.html
end
