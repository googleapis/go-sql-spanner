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
"""Noxfile for spannerlib-python package."""

import glob
import os
import platform
import shutil
from typing import List

import nox

DEFAULT_PYTHON_VERSION = "3.11"

TEST_PYTHON_VERSIONS: List[str] = [
    "3.10",
    "3.11",
    "3.12",
    "3.13",
]


FLAKE8_VERSION = "flake8>=6.1.0,<7.3.0"
BLACK_VERSION = "black[jupyter]>=23.7.0,<25.11.0"
ISORT_VERSION = "isort>=5.11.0,<7.0.0"
LINT_PATHS = ["google", "tests", "samples", "noxfile.py"]

STANDARD_DEPENDENCIES = []

UNIT_TEST_STANDARD_DEPENDENCIES = [
    "pytest",
    "pytest-cov",
    "pytest-asyncio",
]

SYSTEM_TEST_STANDARD_DEPENDENCIES = [
    "pytest",
]

MOCK_TEST_STANDARD_DEPENDENCIES = [
    "pytest",
    "spannermockserver",
]

VERBOSE = True
MODE = "--verbose" if VERBOSE else "--quiet"

DIST_DIR = "dist"
LIB_DIR = "google/cloud/spannerlib/internal/lib"

# Error if a python version is missing
nox.options.error_on_missing_interpreters = True

nox.options.sessions = ["format", "lint", "unit", "mock", "system"]


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
        "--max-line-length=80",
        *LINT_PATHS,
    )


@nox.session(python=TEST_PYTHON_VERSIONS)
def unit(session):
    """Run unit tests."""

    session.install(*STANDARD_DEPENDENCIES, *UNIT_TEST_STANDARD_DEPENDENCIES)
    session.install("-e", ".")

    test_paths = (
        session.posargs if session.posargs else [os.path.join("tests", "unit")]
    )
    session.run(
        "py.test",
        MODE,
        f"--junitxml=unit_{session.python}_sponge_log.xml",
        "--cov=google",
        "--cov=tests/unit",
        "--cov-append",
        "--cov-config=.coveragerc",
        "--cov-report=",
        "--cov-fail-under=80",
        *test_paths,
        env={},
    )


def run_bash_script(session, script_path):
    """Runs a bash script, handling Windows specifics."""
    cmd = ["bash", script_path]

    if platform.system() == "Windows":
        bash_path = shutil.which("bash")
        if not bash_path:
            possible_paths = [
                os.path.join(
                    os.environ.get("ProgramFiles", r"C:\Program Files"),
                    "Git",
                    "bin",
                    "bash.exe",
                ),
                os.path.join(
                    os.environ.get(
                        "ProgramFiles(x86)", r"C:\Program Files (x86)"
                    ),
                    "Git",
                    "bin",
                    "bash.exe",
                ),
            ]
            for p in possible_paths:
                if os.path.exists(p):
                    bash_path = p
                    break

        if bash_path:
            cmd[0] = bash_path
        else:
            session.error(
                "Bash not found. Please ensure 'bash' is in your PATH "
                "or Git Bash is installed."
            )

    session.run(*cmd, external=True)


def _build_artifacts(session):
    """Helper to build spannerlib artifacts."""
    session.log("Building spannerlib artifacts...")
    session.env["RUNNER_OS"] = platform.system()
    run_bash_script(session, "./build-shared-lib.sh")


DEFAULT_DIALECT = ["GOOGLE_STANDARD_SQL", "POSTGRESQL"]


@nox.session(python=TEST_PYTHON_VERSIONS)
@nox.parametrize("dialect", DEFAULT_DIALECT)
def mock(session, dialect):
    """Run mock tests using spannermockserver."""
    # Build/Copy artifacts using the script
    _build_artifacts(session)

    session.install(*STANDARD_DEPENDENCIES, *MOCK_TEST_STANDARD_DEPENDENCIES)
    session.install("-e", ".")

    test_paths = (
        session.posargs if session.posargs else [os.path.join("tests", "mock")]
    )

    session.run(
        "py.test",
        MODE,
        f"--junitxml=mock_googlesql_{session.python}_sponge_log.xml",
        *test_paths,
        env={"SPANNER_DATABASE_DIALECT": dialect},
    )


@nox.session(python=TEST_PYTHON_VERSIONS)
def system(session):
    """Run system tests."""

    # Sanity check: Only run tests if the environment variable is set.
    if not os.environ.get(
        "GOOGLE_APPLICATION_CREDENTIALS", ""
    ) and not os.environ.get("SPANNER_EMULATOR_HOST", ""):
        session.skip(
            "Credentials or emulator host must be set via environment variable"
        )

    # Build/Copy artifacts using the script
    _build_artifacts(session)

    session.install(*STANDARD_DEPENDENCIES, *SYSTEM_TEST_STANDARD_DEPENDENCIES)
    session.install("-e", ".")

    test_paths = (
        session.posargs
        if session.posargs
        else [os.path.join("tests", "system")]
    )
    session.run(
        "py.test",
        MODE,
        f"--junitxml=system_{session.python}_sponge_log.xml",
        *test_paths,
        env={},
    )


@nox.session
def build_spannerlib(session):
    """
    Build SpannerLib artifacts.
    Used only in dev env to build SpannerLib artifacts.
    """
    _build_artifacts(session)


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
    _build_artifacts(session)

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
