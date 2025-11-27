#  Copyright 2025 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Noxfile for spanner-mockserver-python package."""

import glob
import os
import shutil
import tempfile

import nox

DEFAULT_PYTHON_VERSION = "3.13"
PYTHON_VERSIONS = ["3.13"]

VERBOSE = True
MODE = "--verbose" if VERBOSE else "--quiet"

DIST_DIR = "dist"

FLAKE8_VERSION = "flake8>=6.1.0,<7.3.0"
BLACK_VERSION = "black[jupyter]>=23.7.0,<25.11.0"
ISORT_VERSION = "isort>=5.11.0,<7.0.0"

LINT_PATHS = ["spannermockserver", "noxfile.py"]
SKIP_PATHS = ["spannermockserver/generated"]

STANDARD_DEPENDENCIES = [
    "google-cloud-spanner",
    "grpcio",
    "google-api-core",
    "protobuf",
]

nox.options.sessions = ["format", "lint"]

# Error if a python version is missing
nox.options.error_on_missing_interpreters = True


@nox.session(python=DEFAULT_PYTHON_VERSION)
def format(session):
    """
    Run isort to sort imports. Then run black
    to format code to uniform standard.
    """
    session.install(BLACK_VERSION, ISORT_VERSION)
    session.run(
        "isort",
        "--fss",
        "--skip",
        *SKIP_PATHS,
        *LINT_PATHS,
    )
    session.run(
        "black",
        "--line-length=80",
        "--extend-exclude",
        *SKIP_PATHS,
        *LINT_PATHS,
    )


@nox.session
def lint(session):
    """Run linters.

    Returns a failure if the linters find linting errors or sufficiently
    serious code quality issues.
    """
    session.install(FLAKE8_VERSION)
    session.run(
        "flake8",
        "--max-line-length=124",
        "--extend-exclude",
        *SKIP_PATHS,
        *LINT_PATHS,
    )


@nox.session
def build(session):
    """
    Prepares the platform-specific artifacts and builds the wheel.
    """
    if os.path.exists(DIST_DIR):
        shutil.rmtree(DIST_DIR)

    # Install build dependencies
    session.install("build", "twine")

    # Build the wheel
    session.log("Building...")
    session.run("python", "-m", "build")

    # Check the built artifacts with twine
    session.log("Checking artifacts with twine...")
    artifacts = glob.glob("dist/*")
    if not artifacts:
        session.error("No built artifacts found in dist/ to check.")

    session.run("twine", "check", *artifacts)


@nox.session
def install(session):
    """
    Install locally
    """
    build(session)
    session.install(*STANDARD_DEPENDENCIES)
    session.install("-e", ".")


@nox.session
def publish(session):
    """
    Publish to PyPI
    """
    build(session)
    session.install("twine")
    session.run("twine", "upload", "dist/*")


@nox.session
def generate_grpc(session):
    """
    Generate gRPC code from googleapis.
    """
    session.install("grpcio-tools")

    with tempfile.TemporaryDirectory() as tmp_dir:
        # Clone googleapis
        session.log("Cloning googleapis...")
        session.run(
            "git",
            "clone",
            "--depth",
            "1",
            "https://github.com/googleapis/googleapis.git",
            f"{tmp_dir}/googleapis",
            external=True,
        )

        googleapis_dir = os.path.join(tmp_dir, "googleapis")
        proto_files = glob.glob(
            os.path.join(googleapis_dir, "google/spanner/v1/*.proto")
        ) + glob.glob(
            os.path.join(
                googleapis_dir, "google/spanner/admin/database/v1/*.proto"
            )
        )
        proto_files_rel = [
            os.path.relpath(p, googleapis_dir) for p in proto_files
        ]

        # Run protoc
        session.log("Generating code...")
        with session.chdir(googleapis_dir):
            session.run(
                "python",
                "-m",
                "grpc_tools.protoc",
                "-I",
                ".",
                "--python_out=.",
                "--pyi_out=.",
                "--grpc_python_out=.",
                *proto_files_rel,
            )

        target_dir = os.path.join(os.getcwd(), "spannermockserver", "generated")

        files_to_copy = {
            "spanner_pb2_grpc.py": os.path.join(
                googleapis_dir, "google", "spanner", "v1"
            ),
            "spanner_database_admin_pb2_grpc.py": os.path.join(
                googleapis_dir, "google", "spanner", "admin", "database", "v1"
            ),
        }

        for file_name, source_dir in files_to_copy.items():
            src = os.path.join(source_dir, file_name)
            dst = os.path.join(target_dir, file_name)
            shutil.copy(src, dst)
            session.log(f"Copied {file_name} to {target_dir}")
