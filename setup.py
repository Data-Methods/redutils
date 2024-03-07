from struct import pack
from setuptools import setup, find_packages


def get_version():
    with open("VERSION.txt") as f:
        return f.read().strip()


setup(
    name="redutils",
    version=get_version(),
    python_requires=">3.11.0",
    packages=find_packages(where=".", exclude=["./docs", "./.venv"]),
    description="A collection of utilities and apis related to gcu",
)
