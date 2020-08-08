#!/usr/bin/env python

from setuptools import setup

VERSION = "0.1.0a0"

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="target_bigquery",
    version="0.1.0a0",
    description="Google BigQuery target of singer.io framework.",
    author="Daigo Tanaka, Anelen Co., LLC",
    url="https://github.com/anelendata/target_bigquery",
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",

        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",

        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],

    install_requires=["jsonschema==2.6.0",  # singer-pythin requires exact
                      "singer-python>=1.5.0",
                      "google-api-python-client>=1.6.2",
                      "google-cloud>=0.34.0",
                      "google-cloud-bigquery>=1.9.0",
                      "oauth2client"],
      entry_points="""
          [console_scripts]
          target_bigquery=target_bigquery:main
      """,
    packages=["target_bigquery"],
    package_data = {
        # Use MANIFEST.ini
    },
    include_package_data=True
)
