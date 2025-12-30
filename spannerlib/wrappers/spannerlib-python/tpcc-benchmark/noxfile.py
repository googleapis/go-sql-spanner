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
"""
Noxfile for tpcc-benchmark package.

This file defines the nox sessions for linting and formatting the code.
"""

import os

import nox

DEFAULT_PYTHON_VERSION = "3.13"

FLAKE8_VERSION = "flake8>=6.1.0,<7.3.0"
BLACK_VERSION = "black[jupyter]>=23.7.0,<25.11.0"
ISORT_VERSION = "isort>=5.11.0,<7.0.0"

LINT_PATHS = ["tpcc_benchmark", "noxfile.py"]
SKIP_PATHS = ["pytpcc"]

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
        "--max-line-length=80",
        "--extend-exclude",
        *SKIP_PATHS,
        *LINT_PATHS,
    )


@nox.session(python=DEFAULT_PYTHON_VERSION)
@nox.parametrize(
    "driver",
    ["google.cloud.spanner_dbapi", "google.cloud.spanner_python_driver"],
)
def benchmark(session, driver):
    """
    Run the TPC-C benchmark with the specified driver.

    Args:
        session: The nox session object.
        driver: The driver to use for the benchmark.
    """
    session.install("-r", "requirements.txt")

    # Attempt to install the local driver if running with the new driver
    # This assumes the directory structure is consistent
    if driver == "google.cloud.spanner_python_driver":
        session.install("../spanner-python-driver")

    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        session.error("GOOGLE_APPLICATION_CREDENTIALS must be set.")

    args = list(session.posargs)

    # Helper to check if arg exists
    def has_arg(name):
        return any(a.startswith(name) for a in args)

    if not has_arg("--project"):
        if "SPANNER_PROJECT_ID" in os.environ:
            args.extend(["--project", os.environ["SPANNER_PROJECT_ID"]])
        elif "GOOGLE_CLOUD_PROJECT" in os.environ:
            args.extend(["--project", os.environ["GOOGLE_CLOUD_PROJECT"]])
        else:
            session.error(
                "Missing --project or "
                "SPANNER_PROJECT_ID/GOOGLE_CLOUD_PROJECT env var."
            )

    if not has_arg("--instance"):
        if "SPANNER_INSTANCE_ID" in os.environ:
            args.extend(["--instance", os.environ["SPANNER_INSTANCE_ID"]])
        else:
            session.error("Missing --instance or SPANNER_INSTANCE_ID env var.")

    if not has_arg("--database"):
        if "SPANNER_DATABASE_ID" in os.environ:
            args.extend(["--database", os.environ["SPANNER_DATABASE_ID"]])
        else:
            session.error("Missing --database or SPANNER_DATABASE_ID env var.")

    session.run(
        "python",
        "tpcc_benchmark/run_benchmark.py",
        "--driver",
        driver,
        *args,
    )
