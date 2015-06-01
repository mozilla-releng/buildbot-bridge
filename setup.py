from setuptools import setup

setup(
    name="bbb",
    version="1.0",
    description="Buildbot <-> Taskcluster Bridge",
    author="Mozilla Release Engineering",
    packages=["bbb"],
    entry_points={
        "console_scripts": [
            "buildbot-bridge = bbb.runner:main",
        ],
    },
    install_requires=[
        # Because taskcluster hard pins this version...
        "requests==2.4.3",
        "arrow",
        "taskcluster>=0.0.16",
        "sqlalchemy",
        "kombu",
        "redo",
        "mysql-python",
    ],
)
