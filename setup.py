from setuptools import setup

setup(
    name="bbb",
    version="0.1",
    description="Buildbot <-> Taskcluster Bridge",
    author="Mozilla Release Engineering",
    py_modules=["bbb"],
    install_requires=[
        "arrow",
        "taskcluster",
        "sqlalchemy",
        "requests",
        "mozillapulse",
        "redo",
        "mysql-python",
    ],
)
