# Spannerlib Ruby Wrapper

This Ruby wrapper provides an interface for interacting with Google Cloud Spanner via native code, leveraging [FFI (Foreign Function Interface)](https://github.com/ffi/ffi) to connect Ruby with native libraries—such as C or Go (compiled via cgo). It is part of the [`go-sql-spanner`](https://github.com/googleapis/go-sql-spanner) project and enables Ruby applications to utilize high-performance, native Spanner features.

## What Does This Wrapper Do?

- **FFI Integration:** Bridges Ruby with native C (or Go via cgo) code by wrapping native functions using FFI.
- **High Performance:** Provides access to Spanner's performance and reliability through idiomatic Ruby APIs.
- **Mock Server & Testing:** Includes mock server and integration tests to validate functionality.

## Getting Started

### Prerequisites

- Ruby (>= 2.6)
- [Bundler](https://bundler.io/): `gem install bundler`
- [FFI Gem](https://github.com/ffi/ffi) (included in Gemfile)
- Go (if building native libraries from source)

### Setup

Install Ruby dependencies:

```sh
bundle install
```

## FFI & Native Integration

This wrapper uses the [FFI Ruby gem](https://github.com/ffi/ffi) to load and call native functions implemented in C (or Go via cgo).

FFI setup is located in [`lib/spannerlib/ffi.rb`](lib/spannerlib/ffi.rb).  
Refer to the source code [`lib/spannerlib/ffi.rb`](lib/spannerlib/ffi.rb) for details on function signatures and internals.

## Running Tests

### Mock Server Tests

These tests run against an in-memory GRPC mock server.

```sh
bundle exec rspec spec/spannerlib_ruby_spec.rb
```

### Integration Tests

Integration tests are run against the [Google Spanner Emulator](https://cloud.google.com/spanner/docs/emulator).

Start the emulator using Docker (recommended):

```sh
docker run --rm -d \
  -p 9010:9010 \
  gcr.io/cloud-spanner-emulator/emulator
```

Then set the environment variable to direct client library calls to the emulator:

```sh
export SPANNER_EMULATOR_HOST=localhost:9010
bundle exec rspec spec/integration/
```

## Linting & Fixes

To check and auto-correct Ruby code style:

```sh
bundle exec rubocop         # Checks lint
bundle exec rubocop -A      # Auto-corrects where possible
```

If any linting issues remain, resolve those manually.

## Contributing

We welcome contributions! Please follow these steps:

1. Fork and clone this repository.
2. Install dependencies with `bundle install`.
3. Build any necessary native libraries if required.
4. Run linting and tests before submitting a PR.
5. Use clear commit messages (e.g., `chore(ruby): ...`).

Feel free to open issues or pull requests for improvements.

## Troubleshooting

- **FFI Load Errors:** Ensure required native libraries are built and in your library path.
- **Missing Gems:** Run `bundle install`.
- **Spanner Credentials Issues:** Set up [Application Default Credentials](https://cloud.google.com/docs/authentication/provide-credentials-adc), for example by setting the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.

---

For overall project details, see the main [go-sql-spanner README](../../../README.md).

---

**Reference:**  
For examples of adding new functionality inside the FFI layer, see [PR #655](https://github.com/googleapis/go-sql-spanner/pull/655).

## Building and Packaging the Ruby Gem

Building native binaries for the Ruby wrapper (`spannerlib-ruby`) and publishing them as a Ruby gem is automated via GitHub Actions.

### How it Works

- Native shared libraries for Linux, macOS, and Windows are cross-compiled using GitHub-hosted runners and cross-compilers.
- The binaries are packaged as part of the Ruby gem.
- The process is defined in the [`release-ruby-wrapper.yml`](../../../.github/workflows/release-ruby-wrapper.yml) workflow file:
  - Three main stages:
    1. **Compilation:** Builds platform-specific binaries for Linux, macOS, and Windows.
    2. **Artifact Upload:** Compiled binaries are uploaded for later packaging.
    3. **Gem Packaging and Publishing:** The gem is built from source and published to RubyGems automatically if the workflow is manually dispatched.

Please see the [GitHub Actions workflow file](../../../.github/workflows/release-ruby-wrapper.yml) for full implementation details.

---

**Note:** If you want to manually build the gem locally for testing, you can run the following from within `spannerlib/wrappers/spannerlib-ruby`:

```sh
gem build spannerlib-ruby.gemspec
```

For publishing, refer to [RubyGems guides](https://guides.rubygems.org/publishing/) or use the automated workflow.