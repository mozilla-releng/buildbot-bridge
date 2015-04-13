from setuptools import setup

setup(
    name="bbb",
    version="0.2",
    description="Buildbot <-> Taskcluster Bridge",
    author="Mozilla Release Engineering",
    packages=["bbb", "bbb.services"],
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
        "mozillapulse",
        "redo",
        "mysql-python",
        "nose",
        "mock"
    ],
)
