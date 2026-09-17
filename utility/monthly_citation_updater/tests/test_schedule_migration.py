import copy
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "migration", Path(__file__).parents[1] / "disable_legacy_schedules.py")
migration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migration)


class MigrationTests(unittest.TestCase):
    def setUp(self):
        base = "arn:aws:"
        account = "us-west-2:123456789012:"
        self.function = base + "lambda:" + account + "function:spotlake-monthly-citations"
        self.rule = {"Arn": base + "events:" + account + "rule/" + migration.RULE,
                     "State": "ENABLED", "ScheduleExpression": "cron(0 0 1 * ? *)"}
        self.targets = [
            {"Id": "CitationUpdater", "Arn": self.function, "Input": "{}"},
            {"Id": "DriveUploader", "Arn": base + "batch:" + account + "job-queue/" + migration.QUEUE,
             "RoleArn": "batch-event-role", "BatchParameters": {
                 "JobDefinition": migration.JOB_DEFINITION, "JobName": "monthly-drive-dataset-uploader"}},
        ]
        self.schedules = {
            migration.DRIVE_SCHEDULE: {"Name": migration.DRIVE_SCHEDULE, "GroupName": "default",
                "State": "ENABLED", "ScheduleExpression": "cron(0 0 1 * ? *)",
                "ScheduleExpressionTimezone": "UTC", "FlexibleTimeWindow": {"Mode": "OFF"},
                "Description": "Existing Drive schedule", "ActionAfterCompletion": "NONE",
                "Target": {"Arn": "arn:aws:scheduler:::aws-sdk:batch:submitJob",
                    "RoleArn": "existing-role", "RetryPolicy": {"MaximumRetryAttempts": 185},
                    "Input": json.dumps({"JobName": "monthly-drive-dataset-uploader",
                        "JobQueue": migration.QUEUE, "JobDefinition": migration.JOB_DEFINITION})}},
            migration.CITATION_SCHEDULE: {"Name": migration.CITATION_SCHEDULE, "State": "DISABLED",
                "Target": {"Arn": self.function}},
        }
        self.updates = []

    def fake_aws(self, service, operation, *args):
        if operation == "describe-rule":
            return copy.deepcopy(self.rule)
        if operation == "list-targets-by-rule":
            return {"Targets": copy.deepcopy(self.targets)}
        if operation == "get-schedule":
            return copy.deepcopy(self.schedules[args[1]])
        if operation == "update-schedule":
            value = json.loads(args[1])
            self.updates.append(value)
            self.schedules[value["Name"]] = value
            return {}
        raise AssertionError((service, operation, args))

    def run_migration(self):
        with patch.object(migration, "aws", side_effect=self.fake_aws), patch("builtins.print"):
            migration.migrate()

    def test_disables_without_changing_target_or_other_schedule_settings(self):
        before = copy.deepcopy(self.schedules[migration.DRIVE_SCHEDULE])
        self.run_migration()
        before["State"] = "DISABLED"
        self.assertEqual(self.updates, [before])
        self.run_migration()
        self.assertEqual(len(self.updates), 1)

    def test_disabled_rule_cannot_disable_working_schedulers(self):
        self.rule["State"] = "DISABLED"
        with self.assertRaises(ValueError):
            self.run_migration()
        self.assertEqual(self.updates, [])

    def test_wrong_schedule_cannot_disable_working_schedulers(self):
        self.rule["ScheduleExpression"] = "cron(0 4 1 * ? *)"
        with self.assertRaises(ValueError):
            self.run_migration()
        self.assertEqual(self.updates, [])

    def test_missing_target_cannot_disable_working_schedulers(self):
        self.targets.pop()
        with self.assertRaises(ValueError):
            self.run_migration()
        self.assertEqual(self.updates, [])

    def test_unmigrated_legacy_override_prevents_any_mutation(self):
        target = self.schedules[migration.DRIVE_SCHEDULE]["Target"]
        value = json.loads(target["Input"])
        value["ContainerOverrides"] = {"command": ["special-upload"]}
        target["Input"] = json.dumps(value)
        with self.assertRaises(ValueError):
            self.run_migration()
        self.assertEqual(self.updates, [])


if __name__ == "__main__":
    unittest.main()
