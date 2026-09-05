"""Exercise the runner's public CLI without starting Docker or downloading a JDK."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class ExampleRunnerTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "scripts").mkdir()
        shutil.copy(Path(__file__).with_name("run-example.sh"), self.root / "scripts")
        shutil.copy(Path(__file__).with_name("mill-docker.sh"), self.root / "scripts")
        for name in ("01-kafka-demo", "04-connect-demo", "11-parquet-demo", "12-polars-demo", "13-cmak-demo"):
            directory = self.root / "examples" / name / "docker"
            directory.mkdir(parents=True)
            (directory / "docker-compose.yml").write_text("services: {}\n")
        self.log = self.root / "commands"
        binaries = self.root / "bin"
        binaries.mkdir()
        for executable in (binaries / "docker", self.root / "mill"):
            executable.write_text(
                '#!/usr/bin/env bash\n'
                'printf "%s\\n" "${0##*/}" "$@" >> "$COMMAND_LOG"\n'
            )
            executable.chmod(0o755)
        self.env = dict(os.environ, COMMAND_LOG=str(self.log))
        self.env["PATH"] = str(binaries) + os.pathsep + os.environ["PATH"]
        self.env.pop("MILL_DOCKER", None)

    def run_cli(self, *arguments):
        return subprocess.run(
            [str(self.root / "scripts/run-example.sh"), *arguments],
            cwd=self.temporary.name,
            env=self.env,
            text=True,
            capture_output=True,
            check=False,
        )

    def commands(self):
        return self.log.read_text().splitlines() if self.log.exists() else []

    def test_down_preserves_volumes(self):
        self.assertEqual(self.run_cli("down", "01").returncode, 0)
        self.assertEqual(self.commands()[-1], "down")
        self.assertNotIn("-v", self.commands())

    def test_reset_explicitly_removes_volumes(self):
        self.assertEqual(self.run_cli("reset", "01-kafka-demo").returncode, 0)
        self.assertEqual(self.commands()[-2:], ["down", "-v"])

    def test_connect_assembles_plugin_before_starting(self):
        self.assertEqual(self.run_cli("up", "04").returncode, 0)
        self.assertEqual(self.commands()[:3], ["mill", "examples.04-connect-demo.assembly", "docker"])

    def test_batch_job_is_not_started_with_service_readiness(self):
        self.assertNotEqual(self.run_cli("up", "12").returncode, 0)
        self.assertEqual(self.commands(), [])

    def test_minio_setup_runs_as_a_batch_after_readiness(self):
        self.assertEqual(self.run_cli("up", "11").returncode, 0)
        self.assertEqual(self.commands()[-3:], ["run", "--rm", "minio-init"])

    def test_cmak_registration_runs_as_a_batch_after_readiness(self):
        self.assertEqual(self.run_cli("up", "13").returncode, 0)
        self.assertEqual(self.commands()[-3:], ["run", "--rm", "cmak-register"])

    def test_module_path_cannot_escape_examples(self):
        self.assertNotEqual(self.run_cli("reset", "../scripts").returncode, 0)
        self.assertEqual(self.commands(), [])

    def test_program_arguments_remain_literal_and_separate(self):
        arguments = ("seed", "two words", "$(touch unexpected)")
        self.assertEqual(self.run_cli("run", "01", *arguments).returncode, 0)
        self.assertEqual(self.commands()[1:], ["examples.01-kafka-demo.run", *arguments])
        self.assertFalse((self.root / "unexpected").exists())

    def test_container_mode_dispatches_mill_task_through_compose(self):
        self.env["MILL_DOCKER"] = "1"
        self.assertEqual(self.run_cli("test", "01").returncode, 0)
        self.assertEqual(self.commands()[-4:], ["run", "--rm", "mill", "examples.01-kafka-demo.test"])

    def test_extra_arguments_do_not_silently_change_lifecycle_commands(self):
        self.assertNotEqual(self.run_cli("down", "01", "-v").returncode, 0)
        self.assertEqual(self.commands(), [])


if __name__ == "__main__":
    unittest.main()
