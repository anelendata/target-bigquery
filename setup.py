#!/usr/bin/env python
from setuptools import setup

VERSION = "0.2.2"

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="target-bigquery-partition",
    version=VERSION,
    description="Google BigQuery target of singer.io framework.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Daigo Tanaka, Anelen Co., LLC",
    url="https://github.com/anelendata/target-bigquery",

    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",

        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",

        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],

    install_requires=[
        "singer-python==6.0.0",
        "google-api-python-client==2.133.0",
        "google-cloud-bigquery==2.34.2",
        "simplejson==3.11.1",
    ],

    entry_points="""
    [console_scripts]
    target-bigquery=target_bigquery:main
    """,
    packages=["target_bigquery"],
    package_data={
        # Use MANIFEST.ini
    },
    include_package_data=True
)
