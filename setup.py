import os

from setuptools import find_packages, setup

REQUIRES = [
    "flask==1.1.2",
    "requests>=2.24.0,<3",
    "jsonschema==3.2.0",
    "parsl>=1.1.0a0",
    "configobj==5.0.6",
    "texttable>=1.6.4,<2",
    "redis==3.5.3",
    "funcx-common[redis,boto3]==0.0.9",
    "funcx @ git+https://github.com/funcx-faas/funcX.git@main#egg=funcx&subdirectory=funcx_sdk",  # noqa: E501
    "funcx-endpoint @ git+https://github.com/funcx-faas/funcX.git@main#egg=funcx-endpoint&subdirectory=funcx_endpoint",  # noqa: E501
    "pyzmq==22.0.3",
    "pika==1.2.0",
    "python-json-logger==2.0.1",
]

DEV_REQUIRES = [
    "flake8<4",
    "pytest<7",
    "pytest-cov<3",
    "safety",
]

version_ns = {}
with open(os.path.join("funcx_forwarder", "version.py")) as f:
    exec(f.read(), version_ns)
version = version_ns["VERSION"]


setup(
    name="funcx_forwarder",
    version=version,
    packages=find_packages(),
    description="funcX Forwarder: High Performance Function Serving for Science",
    install_requires=REQUIRES,
    python_requires=">=3.6",
    extras_require={"dev": DEV_REQUIRES},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Topic :: Scientific/Engineering",
    ],
    keywords=["funcX", "FaaS", "Function Serving"],
    entry_points={
        "console_scripts": [
            "forwarder-service=funcx_forwarder.service:cli",
        ]
    },
    author="funcX team",
    author_email="labs@globus.org",
    license="Apache License, Version 2.0",
    url="https://github.com/funcx-faas/funcx",
)
