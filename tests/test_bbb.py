
from click.testing import CliRunner

from bbb.cli import reflector


def test_reflector():
    runner = CliRunner()
    result = runner.invoke(reflector, [])

    assert result.output == """Usage: reflector [OPTIONS]

Error: Missing option "--config".
"""
    assert result.exit_code == 2
