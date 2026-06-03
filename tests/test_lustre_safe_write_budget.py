from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "lustre_safe_write_budget.py"
SPEC = importlib.util.spec_from_file_location("lustre_safe_write_budget", SCRIPT_PATH)
assert SPEC is not None
budget = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules["lustre_safe_write_budget"] = budget
SPEC.loader.exec_module(budget)


def test_parse_size_supports_binary_and_decimal_units():
    assert budget.parse_size("1GiB") == 1024**3
    assert budget.parse_size("1.5TiB") == int(1.5 * 1024**4)
    assert budget.parse_size("2GB") == 2 * 1000**3
    assert budget.parse_size("123") == 123


def test_parse_size_rejects_unknown_units():
    with pytest.raises(ValueError, match="unsupported size unit"):
        budget.parse_size("1XB")


def test_calculate_budget_allows_plan_under_80_percent_cap():
    result = budget.calculate_budget(
        capacity_bytes=1000,
        used_bytes=600,
        planned_write_bytes=100,
        safety_reserve_bytes=50,
        max_used_percent=80,
    )

    assert result.allowed is True
    assert result.max_allowed_used_bytes == 800
    assert result.safe_write_budget_bytes == 150
    assert result.projected_used_bytes == 700
    assert result.projected_used_percent == 70


def test_calculate_budget_blocks_plan_that_exceeds_cap():
    result = budget.calculate_budget(
        capacity_bytes=1000,
        used_bytes=700,
        planned_write_bytes=101,
        safety_reserve_bytes=0,
        max_used_percent=80,
    )

    assert result.allowed is False
    assert result.safe_write_budget_bytes == 100
    assert result.reason == "planned write exceeds the safe write budget"


def test_calculate_budget_blocks_when_already_above_safe_budget():
    result = budget.calculate_budget(
        capacity_bytes=1000,
        used_bytes=850,
        planned_write_bytes=0,
        safety_reserve_bytes=0,
        max_used_percent=80,
    )

    assert result.allowed is False
    assert result.safe_write_budget_bytes == 0
    assert result.reason == "filesystem is already above the safe write budget"


def test_main_returns_nonzero_when_plan_is_not_allowed(capsys):
    exit_code = budget.main(
        [
            "--capacity",
            "1000B",
            "--used",
            "700B",
            "--planned-write",
            "101B",
            "--max-used-percent",
            "80",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "Allowed: False" in captured.out
