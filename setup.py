from setuptools import setup, find_packages

setup(
    name="spark_databricks_project",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyspark>=3.5.0",
        "delta-spark>=3.1.0",
    ],
    python_requires=">=3.9",
)
