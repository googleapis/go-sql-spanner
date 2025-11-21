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

import glob
import os
import platform
import shutil
from typing import List

import nox

DEFAULT_PYTHON_VERSION = "3.13"
PYTHON_VERSIONS = ["3.13"]

UNIT_TEST_PYTHON_VERSIONS: List[str] = ["3.13"]
SYSTEM_TEST_PYTHON_VERSIONS: List[str] = ["3.13"]


FLAKE8_VERSION = "flake8>=6.1.0,<7.0.0"
BLACK_VERSION = "black[jupyter]>=23.7.0,<24.0.0"
ISORT_VERSION = "isort>=5.11.0,<6.0.0"
LINT_PATHS = ["google", "tests", "samples", "noxfile.py"]

STANDARD_DEPENDENCIES = [
    "google-cloud-spanner",
]

UNIT_TEST_STANDARD_DEPENDENCIES = [
    "mock",
    "asyncmock",
    "pytest",
    "pytest-cov",
    "pytest-asyncio",
]

SYSTEM_TEST_STANDARD_DEPENDENCIES = [
    "pytest",
]

VERBOSE = True
MODE = "--verbose" if VERBOSE else "--quiet"

DIST_DIR = "dist"
LIB_DIR = "google/cloud/spannerlib/internal/lib"
ARTIFACT_DIR = "spannerlib-artifacts"

# Error if a python version is missing
nox.options.error_on_missing_interpreters = True

nox.options.sessions = ["format", "lint", "unit", "system"]


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
        *LINT_PATHS,
    )
    session.run(
        "black",
        "--line-length=80",
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
        *LINT_PATHS,
    )


@nox.session(python=UNIT_TEST_PYTHON_VERSIONS)
def unit(session):
    """Run unit tests."""

    session.install(*STANDARD_DEPENDENCIES, *UNIT_TEST_STANDARD_DEPENDENCIES)

    # Run py.test against the unit tests.
    session.run(
        "py.test",
        MODE,
        f"--junitxml=unit_{session.python}_sponge_log.xml",
        "--cov=google",
        "--cov=tests/unit",
        "--cov-append",
        "--cov-config=.coveragerc",
        "--cov-report=",
        "--cov-fail-under=0",
        os.path.join("tests", "unit"),
        *session.posargs,
        env={},
    )


@nox.session(python=SYSTEM_TEST_PYTHON_VERSIONS)
def system(session):
    """Run system tests."""

    session.install(*STANDARD_DEPENDENCIES, *SYSTEM_TEST_STANDARD_DEPENDENCIES)

    # Run py.test against the unit tests.
    session.run(
        "py.test",
        MODE,
        f"--junitxml=system_{session.python}_sponge_log.xml",
        os.path.join("tests", "system"),
        *session.posargs,
        env={},
    )


def get_spannerlib_artifacts_binary(session):
    """
    Returns spannerlib lib and header files.
    """
    header = "spannerlib.h"

    system = platform.system()
    if system == "Darwin":
        lib, folder = "spannerlib.dylib", "osx-arm64"
    elif system == "Windows":
        lib, folder = "spannerlib.dll", "win-x64"
    elif system == "Linux":
        lib, folder = "spannerlib.so", "linux-x64"
    else:
        session.error(f"Unsupported platform: {system}")
    return (lib, folder, header)


@nox.session
def build_spannerlib(session):
    """
    Build SpannerLib artifacts.
    """
    session.log("Building spannerlib artifacts...")

    # Run the build script
    session.env["RUNNER_OS"] = platform.system()
    session.run("bash", "./build-shared-lib.sh", external=True)


def copy_artifacts(session):
    """
    Copy correct spannerlib artifact to lib folder
    """
    session.log("Copy platform specific artifacts to lib dir")
    artifact_dir_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), ARTIFACT_DIR
    )
    lib_dir_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), LIB_DIR
    )
    if os.path.exists(LIB_DIR):
        shutil.rmtree(LIB_DIR)
    os.makedirs(LIB_DIR)
    lib, folder, header = get_spannerlib_artifacts_binary(session)
    shutil.copy(
        os.path.join(artifact_dir_path, folder, lib),
        os.path.join(lib_dir_path, lib),
    )
    shutil.copy(
        os.path.join(artifact_dir_path, folder, header),
        os.path.join(lib_dir_path, header),
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

    # Run the preparation step
    copy_artifacts(session)

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
    session.install("-e", ".")
