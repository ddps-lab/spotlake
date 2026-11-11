# One-time adoption of resources that were created before this stack kept remote state.
# Remove this file after the adoption apply succeeds.

import {
  to = aws_iam_role.batch_service_role
  id = "aws_batch_service_role_spotlake_azure"
}

import {
  to = aws_iam_role_policy_attachment.batch_service_role_policy
  id = "aws_batch_service_role_spotlake_azure/arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
}

import {
  to = aws_iam_policy.batch_service_ecs_policy
  id = "arn:aws:iam::320674564649:policy/batch_service_ecs_policy_spotlake_azure"
}

import {
  to = aws_iam_role_policy_attachment.batch_service_ecs_policy_attachment
  id = "aws_batch_service_role_spotlake_azure/arn:aws:iam::320674564649:policy/batch_service_ecs_policy_spotlake_azure"
}

import {
  to = aws_iam_role.ecs_task_execution_role
  id = "ecs_task_execution_role_spotlake_azure"
}

import {
  to = aws_iam_role_policy_attachment.ecs_task_execution_role_policy
  id = "ecs_task_execution_role_spotlake_azure/arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

import {
  to = aws_iam_role.batch_job_role
  id = "batch_job_role_spotlake_azure"
}

import {
  to = aws_iam_policy.batch_job_policy
  id = "arn:aws:iam::320674564649:policy/batch_job_policy_spotlake_azure"
}

import {
  to = aws_iam_role_policy_attachment.batch_job_policy_attachment
  id = "batch_job_role_spotlake_azure/arn:aws:iam::320674564649:policy/batch_job_policy_spotlake_azure"
}

import {
  to = aws_iam_role.ecs_instance_role
  id = "ecs_instance_role_spotlake_azure"
}

import {
  to = aws_iam_role_policy_attachment.ecs_instance_role_policy
  id = "ecs_instance_role_spotlake_azure/arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

import {
  to = aws_iam_instance_profile.ecs_instance_role
  id = "ecs_instance_role_profile_spotlake_azure"
}

import {
  to = aws_batch_compute_environment.spot_compute_env
  id = "spotlake-azure-compute-env"
}

import {
  to = aws_batch_job_queue.spot_job_queue
  id = "arn:aws:batch:us-west-2:320674564649:job-queue/spotlake-azure-job-queue"
}

import {
  to = aws_batch_job_definition.collection_job
  id = "arn:aws:batch:us-west-2:320674564649:job-definition/spotlake-azure-collection-job:5"
}

import {
  to = aws_iam_role.scheduler_role
  id = "eventbridge_scheduler_role_spotlake_azure"
}

import {
  to = aws_iam_policy.scheduler_policy
  id = "arn:aws:iam::320674564649:policy/eventbridge_scheduler_policy_spotlake_azure"
}

import {
  to = aws_iam_role_policy_attachment.scheduler_policy_attachment
  id = "eventbridge_scheduler_role_spotlake_azure/arn:aws:iam::320674564649:policy/eventbridge_scheduler_policy_spotlake_azure"
}

import {
  to = aws_cloudwatch_event_rule.collection_schedule
  id = "spotlake-azure-collection-schedule"
}

import {
  to = aws_cloudwatch_event_target.collection_target
  id = "spotlake-azure-collection-schedule/spotlake-azure-collection-target"
}

import {
  to = aws_iam_role.batch_notifier_lambda_role[0]
  id = "batch-failure-notifier-role"
}

import {
  to = aws_iam_role_policy_attachment.lambda_basic[0]
  id = "batch-failure-notifier-role/arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

import {
  to = aws_lambda_function.batch_failure_notifier[0]
  id = "batch-failure-notifier"
}

import {
  to = aws_cloudwatch_event_rule.batch_job_failure[0]
  id = "azure-batch-job-failure"
}

import {
  to = aws_cloudwatch_event_target.batch_failure_to_lambda[0]
  id = "azure-batch-job-failure/BatchFailureNotifierLambda"
}

import {
  to = aws_lambda_permission.allow_eventbridge[0]
  id = "batch-failure-notifier/AllowExecutionFromEventBridge_azure-batch-job-failure"
}
