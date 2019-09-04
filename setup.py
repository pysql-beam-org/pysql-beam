"""
Making the module installable

"""

import setuptools
with open("./README.md", "r") as fh:
    readme = fh.read()

with open("./requirements.txt", "r") as dependency:
    REQUIRED_PACKAGES = dependency.read().split("\n")

version = "0.2"

setuptools.setup(
    name='pysql-beam',
    version=version,
    author="Deepak Verma",
    author_email="yesdeepakverma@gmail.com",
    description="Apache beam mysql and postgres io connector in pure python",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://bitbucket.org/dverma90/pysql-beam",
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3",
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        "Operating System :: OS Independent",
    ],
    keywords="apache-beam,mysql,postgres,python,io,source,sink,read,write,database",
 )
