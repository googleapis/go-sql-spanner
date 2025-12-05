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
from typing import List

import nox

DEFAULT_PYTHON_VERSION = "3.13"
SYSTEM_TEST_PYTHON_VERSIONS: List[str] = [
    "3.10",
    "3.11",
    "3.12",
    "3.13",
    "3.14",
]

VERBOSE = True
MODE = "--verbose" if VERBOSE else "--quiet"

DIST_DIR = "dist"

FLAKE8_VERSION = "flake8>=6.1.0,<7.3.0"
BLACK_VERSION = "black[jupyter]>=23.7.0,<25.11.0"
ISORT_VERSION = "isort>=5.11.0,<7.0.0"

LINT_PATHS = ["spannermockserver", "noxfile.py"]
SKIP_PATHS = ["spannermockserver/generated"]

STANDARD_DEPENDENCIES = []

SYSTEM_TEST_STANDARD_DEPENDENCIES = [
    "pytest",
]

nox.options.sessions = ["format", "lint", "tests", "noxfile.py"]

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


@nox.session(python=SYSTEM_TEST_PYTHON_VERSIONS)
def system(session):
    """Run system tests."""
    session.install(*STANDARD_DEPENDENCIES, *SYSTEM_TEST_STANDARD_DEPENDENCIES)
    session.install(".")

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
    session.install("-e", ".")


@nox.session
def publish(session):
    """
    Publish to PyPI
    """
    build(session)
    session.install("twine")
    session.run("twine", "upload", "dist/*")
