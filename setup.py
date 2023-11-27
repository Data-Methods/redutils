from struct import pack
from setuptools import setup, find_packages


setup(
    name="pygcu",
    version="0.1",
    python_requires='>3.11.0',
    packages=find_packages(),
    description="A collection of utilities and apis related to gcu",
)
