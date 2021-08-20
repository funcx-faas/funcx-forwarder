import os

from setuptools import find_packages, setup

DEV_REQUIREMENTS = [
    "flake8<4",
    "pytest<7",
    "pytest-cov<3",
    "safety",
]

version_ns = {}
with open(os.path.join("funcx_forwarder", "version.py")) as f:
    exec(f.read(), version_ns)
version = version_ns["VERSION"]
print("Version : ", version)

with open("requirements.txt") as f:
    install_requires = f.readlines()

setup(
    name="funcx_forwarder",
    version=version,
    packages=find_packages(),
    description="funcX Forwarder: High Performance Function Serving for Science",
    install_requires=install_requires,
    python_requires=">=3.6",
    extras_require={"dev": DEV_REQUIREMENTS},
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
