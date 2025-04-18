from setuptools import find_packages, setup

setup(
    name="minimal_lakehouse",
    packages=find_packages(exclude=["minimal_lakehouse_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "minio"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
