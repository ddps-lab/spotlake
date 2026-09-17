"""Verify the shared monthly rule before disabling its two legacy Schedulers.

Uses the AWS CLI credentials/region from the environment, just like deployment.
Never submits a Batch job or invokes Lambda. Safe to rerun after every deployment.
"""
import json
import subprocess

RULE = "spotlake-monthly-jobs"
DRIVE_SCHEDULE = "monthly-drive-dataset-uploader-cron"
CITATION_SCHEDULE = "spotlake-monthly-citations-cron"
JOB_DEFINITION = "monthly_drive_dataset_uploader_definition"
QUEUE = "montly_share_raw_dataset_generator_queue"


def aws(*args):
    return json.loads(subprocess.check_output(["aws", *args, "--output", "json"]))


def migrate():
    rule = aws("events", "describe-rule", "--name", RULE)
    if rule["State"] != "ENABLED" or rule["ScheduleExpression"] != "cron(0 0 1 * ? *)":
        raise ValueError("Shared monthly rule must be enabled at UTC midnight")
    prefix = rule["Arn"].split(":")
    partition, region, account = prefix[1], prefix[3], prefix[4]
    citation_arn = f"arn:{partition}:lambda:{region}:{account}:function:spotlake-monthly-citations"
    queue_arn = f"arn:{partition}:batch:{region}:{account}:job-queue/{QUEUE}"
    targets = aws("events", "list-targets-by-rule", "--rule", RULE)["Targets"]
    target_map = {t["Id"]: t for t in targets}
    citation = target_map.get("CitationUpdater", {})
    drive = target_map.get("DriveUploader", {})
    if (citation.get("Arn") != citation_arn or citation.get("Input") != "{}"
            or drive.get("Arn") != queue_arn or not drive.get("RoleArn")
            or drive.get("BatchParameters") != {
                "JobDefinition": JOB_DEFINITION, "JobName": "monthly-drive-dataset-uploader"}):
        raise ValueError("Shared rule targets do not match the expected Lambda/Batch jobs")

    # Preflight both schedules before changing either. Do not silently discard
    # future overrides someone might have added to the original Drive target.
    schedules = [aws("scheduler", "get-schedule", "--name", name, "--group-name", "default")
                 for name in (DRIVE_SCHEDULE, CITATION_SCHEDULE)]
    old_drive, old_citation = schedules
    if (old_drive["Target"]["Arn"] != "arn:aws:scheduler:::aws-sdk:batch:submitJob"
            or json.loads(old_drive["Target"]["Input"]) != {
                "JobName": "monthly-drive-dataset-uploader", "JobQueue": QUEUE,
                "JobDefinition": JOB_DEFINITION}
            or old_citation["Target"]["Arn"] != citation_arn):
        raise ValueError("Legacy targets changed; review them before disabling")

    fields = ("Name", "GroupName", "ScheduleExpression", "ScheduleExpressionTimezone",
              "StartDate", "EndDate", "State", "Description", "FlexibleTimeWindow",
              "Target", "KmsKeyArn", "ActionAfterCompletion")
    for schedule in schedules:
        if schedule["State"] != "DISABLED":
            update = {k: schedule[k] for k in fields if k in schedule}
            update["State"] = "DISABLED"
            aws("scheduler", "update-schedule", "--cli-input-json", json.dumps(update))
        after = aws("scheduler", "get-schedule", "--name", schedule["Name"], "--group-name", "default")
        if after["State"] != "DISABLED" or after["Target"] != schedule["Target"]:
            raise ValueError("Legacy schedule disable verification failed")
        print(f"{schedule['Name']}: DISABLED; target preserved")
    print(f"{RULE}: ENABLED; both targets verified")


if __name__ == "__main__":
    migrate()
