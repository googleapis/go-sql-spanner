"""Setup script for spannerlib-python package."""
from setuptools import setup


setup(
    has_ext_modules=lambda: True,
    include_package_data=True,
    python_requires=">=3.8"
)
