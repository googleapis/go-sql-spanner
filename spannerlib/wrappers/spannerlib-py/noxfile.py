import os
from typing import List

import nox

DEFAULT_PYTHON_VERSION = "3.13"
PYTHON_VERSIONS = ["3.13"]

UNIT_TEST_PYTHON_VERSIONS: List[str] = ["3.13"]
SYSTEM_TEST_PYTHON_VERSIONS: List[str] = ["3.13"]


FLAKE8_VERSION = "flake8==6.1.0"
BLACK_VERSION = "black[jupyter]==23.7.0"
ISORT_VERSION = "isort==5.11.0"
LINT_PATHS = ["google", "tests", "noxfile.py"]

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

# Error if a python version is missing
nox.options.error_on_missing_interpreters = True


@nox.session(python=DEFAULT_PYTHON_VERSION)
def format(session):
    """
    Run isort to sort imports. Then run black
    to format code to uniform standard.
    """
    session.install(BLACK_VERSION, ISORT_VERSION)
    # Use the --fss option to sort imports using strict alphabetical order.
    # See https://pycqa.github.io/isort/docs/configuration/options.html#force-sort-within-sections
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
    session.install(FLAKE8_VERSION, BLACK_VERSION)
    session.install("black", "isort")
    session.run(
        "flake8",
        "--max-line-length=124",
        *LINT_PATHS,
    )


@nox.session(python=UNIT_TEST_PYTHON_VERSIONS)
def unit(session):
    """Run unit tests."""

    session.install(*UNIT_TEST_STANDARD_DEPENDENCIES)

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

    session.install(*SYSTEM_TEST_STANDARD_DEPENDENCIES)

    # Run py.test against the unit tests.
    session.run(
        "py.test",
        MODE,
        f"--junitxml=system_{session.python}_sponge_log.xml",
        os.path.join("tests", "system"),
        *session.posargs,
        env={},
    )
