from setuptools import setup

setup(
    name="bbb",
    version="0.1",
    description="Buildbot <-> Taskcluster Bridge",
    author="Mozilla Release Engineering",
    py_modules=["bbb"],
    install_requires=[
        # Because taskcluster hard pins this version...
        "requests==2.4.3",
        "arrow",
        "taskcluster",
        "sqlalchemy",
        "mozillapulse",
        "redo",
        "mysql-python",
    ],
)
