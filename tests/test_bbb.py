
from click.testing import CliRunner

from bbb.cli import main


def test_main():
    runner = CliRunner()
    result = runner.invoke(main, [])

    assert result.output == """Usage: main [OPTIONS]

Error: Missing option "--config".
"""
    assert result.exit_code == 2
