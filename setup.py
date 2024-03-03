from struct import pack
from setuptools import setup, find_packages


setup(
    name="redutils",
    version="0.3.1",
    python_requires=">3.11.0",
    packages=find_packages(where=".", exclude=["./docs", "./.venv"]),
    description="A collection of utilities and apis related to gcu",
)
