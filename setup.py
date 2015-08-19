try:
    from setuptools import setup
except:
    from distutils.core import setup

setup(
    name="bbb",
    version="1.1",
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
    tests_require=[
        "mock",
        "flake8",
        "pytest",
        "pytest-cov",
    ],
)
