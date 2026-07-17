"""Helpers for working with SQL Server LSN values."""

from __future__ import annotations

from typing import Optional


def ensure_lsn_bytes(value: object | None) -> Optional[bytes]:
    """Normalize pyodbc binary values to bytes."""
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    raise TypeError(f"Unsupported LSN value type: {type(value)!r}")


def lsn_bytes_to_hex(value: bytes | bytearray | memoryview) -> str:
    raw = ensure_lsn_bytes(value)
    if raw is None:
        raise ValueError("LSN cannot be None")
    return f"0x{raw.hex().upper()}"


def lsn_hex_to_bytes(value: str) -> bytes:
    compact = value[2:] if value.lower().startswith("0x") else value
    compact = compact.strip()
    if len(compact) % 2 != 0:
        raise ValueError(f"Invalid LSN hex string: {value!r}")
    return bytes.fromhex(compact)


def lsn_bytes_to_debezium(value: bytes | bytearray | memoryview) -> str:
    raw = ensure_lsn_bytes(value)
    if raw is None:
        raise ValueError("LSN cannot be None")
    hex_string = raw.hex().upper()
    if len(hex_string) != 20:
        raise ValueError(f"SQL Server LSN must be 10 bytes, got {len(raw)}")
    return f"{hex_string[0:8]}:{hex_string[8:16]}:{hex_string[16:20]}"


def compare_lsn(left: bytes | bytearray | memoryview, right: bytes | bytearray | memoryview) -> int:
    left_bytes = ensure_lsn_bytes(left)
    right_bytes = ensure_lsn_bytes(right)
    if left_bytes is None or right_bytes is None:
        raise ValueError("LSN values for comparison cannot be None")
    if left_bytes < right_bytes:
        return -1
    if left_bytes > right_bytes:
        return 1
    return 0
