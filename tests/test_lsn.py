from __future__ import annotations

import unittest

from sqlserver_cdc_s3.lsn import lsn_bytes_to_debezium, lsn_bytes_to_hex


class LsnTests(unittest.TestCase):
    def test_lsn_formatting(self) -> None:
        value = bytes.fromhex("0000002700000AC00002")
        self.assertEqual(lsn_bytes_to_hex(value), "0x0000002700000AC00002")
        self.assertEqual(lsn_bytes_to_debezium(value), "00000027:00000AC0:0002")


if __name__ == "__main__":
    unittest.main()
