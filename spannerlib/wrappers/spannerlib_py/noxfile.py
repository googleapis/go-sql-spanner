import nox

PYTHON_VERSIONS = ["3.13"]


@nox.session(python=PYTHON_VERSIONS)
def tests(session):
    session.install("pytest")
    session.run("pytest")


@nox.session
def lint(session):
    session.install("flake8", "black")
    session.run("black", "--check", ".")
    session.run("flake8", "your_package", "tests")


@nox.session
def format(session):
    session.install("black", "isort")
    session.run("isort", ".")
    session.run("black", ".")
