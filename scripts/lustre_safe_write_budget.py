#!/usr/bin/env python3
"""Calculate a safe write budget for Azure Managed Lustre pressure tests."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass

_BINARY_UNITS = {
    "b": 1,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
    "pib": 1024**5,
}
_DECIMAL_UNITS = {
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
    "pb": 1000**5,
}
_SIZE_RE = re.compile(r"^\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>[a-zA-Z]+)?\s*$")


@dataclass(frozen=True)
class SafeWriteBudget:
    capacity_bytes: int
    used_bytes: int
    max_used_percent: float
    safety_reserve_bytes: int
    planned_write_bytes: int
    max_allowed_used_bytes: int
    safe_write_budget_bytes: int
    projected_used_bytes: int
    projected_used_percent: float
    allowed: bool
    reason: str


def parse_size(value: str) -> int:
    """Parse a storage size such as 500GiB, 1.5TiB, or 100GB into bytes."""
    match = _SIZE_RE.match(value)
    if not match:
        raise ValueError(f"invalid size: {value!r}")

    number = float(match.group("value"))
    unit = (match.group("unit") or "b").lower()
    multiplier = _BINARY_UNITS.get(unit) or _DECIMAL_UNITS.get(unit)
    if multiplier is None:
        raise ValueError(f"unsupported size unit: {unit!r}")
    return int(number * multiplier)


def format_bytes(value: int) -> str:
    """Format bytes as a compact binary unit string."""
    sign = "-" if value < 0 else ""
    remaining = abs(float(value))
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if remaining < 1024 or unit == "PiB":
            if unit == "B":
                return f"{sign}{int(remaining)} {unit}"
            return f"{sign}{remaining:.2f} {unit}"
        remaining /= 1024
    return f"{value} B"


def calculate_budget(
    *,
    capacity_bytes: int,
    used_bytes: int,
    max_used_percent: float = 80.0,
    safety_reserve_bytes: int = 0,
    planned_write_bytes: int = 0,
) -> SafeWriteBudget:
    """Calculate whether a planned write fits inside the configured safety cap."""
    if capacity_bytes <= 0:
        raise ValueError("capacity_bytes must be greater than zero")
    if used_bytes < 0:
        raise ValueError("used_bytes must be non-negative")
    if safety_reserve_bytes < 0:
        raise ValueError("safety_reserve_bytes must be non-negative")
    if planned_write_bytes < 0:
        raise ValueError("planned_write_bytes must be non-negative")
    if max_used_percent <= 0 or max_used_percent > 100:
        raise ValueError("max_used_percent must be in the range (0, 100]")

    max_allowed_used_bytes = int(capacity_bytes * (max_used_percent / 100.0))
    safe_write_budget_bytes = max_allowed_used_bytes - used_bytes - safety_reserve_bytes
    projected_used_bytes = used_bytes + planned_write_bytes
    projected_used_percent = projected_used_bytes / capacity_bytes * 100.0

    if safe_write_budget_bytes < 0:
        allowed = False
        reason = "filesystem is already above the safe write budget"
    elif planned_write_bytes > safe_write_budget_bytes:
        allowed = False
        reason = "planned write exceeds the safe write budget"
    else:
        allowed = True
        reason = "planned write fits within the safe write budget"

    return SafeWriteBudget(
        capacity_bytes=capacity_bytes,
        used_bytes=used_bytes,
        max_used_percent=max_used_percent,
        safety_reserve_bytes=safety_reserve_bytes,
        planned_write_bytes=planned_write_bytes,
        max_allowed_used_bytes=max_allowed_used_bytes,
        safe_write_budget_bytes=max(0, safe_write_budget_bytes),
        projected_used_bytes=projected_used_bytes,
        projected_used_percent=projected_used_percent,
        allowed=allowed,
        reason=reason,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Calculate safe Lustre pressure-test write budget."
    )
    parser.add_argument("--capacity", required=True, help="Filesystem capacity, e.g. 4TiB")
    parser.add_argument("--used", required=True, help="Currently used capacity, e.g. 2.1TiB")
    parser.add_argument(
        "--planned-write",
        default="0B",
        help="Planned maximum write size for this run, e.g. 500GiB",
    )
    parser.add_argument(
        "--reserve",
        default="0B",
        help="Additional safety reserve to subtract from the budget, e.g. 100GiB",
    )
    parser.add_argument(
        "--max-used-percent",
        type=float,
        default=80.0,
        help="Hard maximum used percentage. Default: 80",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = calculate_budget(
        capacity_bytes=parse_size(args.capacity),
        used_bytes=parse_size(args.used),
        planned_write_bytes=parse_size(args.planned_write),
        safety_reserve_bytes=parse_size(args.reserve),
        max_used_percent=args.max_used_percent,
    )

    if args.json:
        print(json.dumps(asdict(result), indent=2, sort_keys=True))
    else:
        print(f"Allowed: {result.allowed}")
        print(f"Reason: {result.reason}")
        print(f"Capacity: {format_bytes(result.capacity_bytes)}")
        print(f"Used: {format_bytes(result.used_bytes)}")
        print(f"Max allowed used: {format_bytes(result.max_allowed_used_bytes)}")
        print(f"Safety reserve: {format_bytes(result.safety_reserve_bytes)}")
        print(f"Safe write budget: {format_bytes(result.safe_write_budget_bytes)}")
        print(f"Planned write: {format_bytes(result.planned_write_bytes)}")
        print(
            "Projected used: "
            f"{format_bytes(result.projected_used_bytes)} "
            f"({result.projected_used_percent:.2f}%)"
        )

    return 0 if result.allowed else 2


if __name__ == "__main__":
    raise SystemExit(main())
