"""Check the native adapter against the same golden contract used by Scala."""

from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class PolarsManifestTest(unittest.TestCase):
    def test_python_implements_the_shared_manifest_contract(self):
        example = Path(__file__).resolve().parent.parent / "examples/12-polars-arrow-bridge"
        expected = (example / "test/resources/manifest.txt").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as directory:
            order_lines = Path(directory) / "order_lines.arrow"
            regions = Path(directory) / "regions.arrow"
            order_lines.write_bytes(b"orders\n")
            regions.write_bytes(b"regions\n")
            result = subprocess.run(
                [sys.executable, str(example / "docker/aggregate.py"), "--input-manifest",
                 str(order_lines), str(regions)],
                check=True, capture_output=True, text=True,
            )
        self.assertEqual(result.stdout, expected)


if __name__ == "__main__":
    unittest.main()
