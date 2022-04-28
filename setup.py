import setuptools

from sqs_mega_python_zwap import __email__, __author__, __version__

with open('requirements.txt') as f:
    requirements = f.read().splitlines()


setuptools.setup(
    name="sqs_mega_python_zwap",
    version=__version__,
    author=__author__,
    author_email=__email__,
    description="Sqs mega python Zwap",
    packages=setuptools.find_packages(),
    package_dir={"sqs_mega_python_zwap": "./sqs_mega_python_zwap"},
    install_requires=requirements,
)
