"""Setup script for spannerlib."""
from setuptools import setup
from wheel.bdist_wheel import bdist_wheel as _bdist_wheel

class MyBDistWheel(_bdist_wheel):
    def get_tag(self):
        """
        Forces the wheel to have the 'py38' and 'abi3' tags.
        """
        _, _, plat = super().get_tag()
        
        python_tag = "py38"  # 'py38' means Python 3.8 or newer
        abi_tag = "abi3"     # 'abi3' means "stable ABI"
        
        return (python_tag, abi_tag, plat)

setup(
    has_ext_modules=lambda: True,
    include_package_data=True,
    cmdclass={
        'bdist_wheel': MyBDistWheel
    },
    python_requires=">=3.8"
)