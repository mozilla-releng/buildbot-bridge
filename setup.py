try:
    from setuptools import setup
except:
    from distutils.core import setup

setup(
    name="bbb",
    version="1.5.17",
    description="Buildbot <-> Taskcluster Bridge",
    author="Mozilla Release Engineering",
    packages=["bbb", "bbb.schemas"],
    entry_points={
        "console_scripts": [
            "buildbot-bridge = bbb.runner:main",
        ],
    },
    # Package contains data files -> not safe.
    zip_safe=False,
    include_package_data=True,
    install_requires=[
        # Because taskcluster hard pins this version...
        "requests==2.4.3",
        "arrow",
        "taskcluster>=0.0.26",
        "sqlalchemy",
        "kombu",
        "redo",
        "mysql-python",
        "jsonschema",
        "PyYAML",
        "slugid",
    ],
    tests_require=[
        "mock",
        "flake8",
        "pytest",
        "pytest-cov",
        "pytest-capturelog",
    ],
)
