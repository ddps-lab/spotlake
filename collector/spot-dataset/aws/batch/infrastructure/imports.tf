# One-time adoption of resources that were created before this stack kept remote state.
# Remove this file after the adoption apply succeeds.

import {
  to = aws_iam_role.batch_service_role
  id = "aws_batch_service_role_spotlake"
}

import {
  to = aws_iam_role_policy_attachment.batch_service_role_policy
  id = "aws_batch_service_role_spotlake/arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
}

import {
  to = aws_iam_policy.batch_service_ecs_policy
  id = "arn:aws:iam::320674564649:policy/batch_service_ecs_policy_spotlake"
}

import {
  to = aws_iam_role_policy_attachment.batch_service_ecs_policy_attachment
  id = "aws_batch_service_role_spotlake/arn:aws:iam::320674564649:policy/batch_service_ecs_policy_spotlake"
}

import {
  to = aws_iam_role.ecs_task_execution_role
  id = "ecs_task_execution_role_spotlake"
}

import {
  to = aws_iam_role_policy_attachment.ecs_task_execution_role_policy
  id = "ecs_task_execution_role_spotlake/arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

import {
  to = aws_iam_role.batch_job_role
  id = "batch_job_role_spotlake"
}

import {
  to = aws_iam_policy.batch_job_policy
  id = "arn:aws:iam::320674564649:policy/batch_job_policy_spotlake"
}

import {
  to = aws_iam_role_policy_attachment.batch_job_policy_attachment
  id = "batch_job_role_spotlake/arn:aws:iam::320674564649:policy/batch_job_policy_spotlake"
}

import {
  to = aws_iam_role.ecs_instance_role
  id = "ecs_instance_role_spotlake"
}

import {
  to = aws_iam_role_policy_attachment.ecs_instance_role_policy
  id = "ecs_instance_role_spotlake/arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

import {
  to = aws_iam_instance_profile.ecs_instance_role
  id = "ecs_instance_role_profile_spotlake"
}

import {
  to = aws_batch_compute_environment.spot_compute_env
  id = "spotlake-aws-compute-env"
}

import {
  to = aws_batch_job_queue.spot_job_queue
  id = "arn:aws:batch:us-west-2:320674564649:job-queue/spotlake-aws-job-queue"
}

import {
  to = aws_batch_job_definition.collection_job
  id = "arn:aws:batch:us-west-2:320674564649:job-definition/spotlake-aws-collection-job:8"
}

import {
  to = aws_iam_role.scheduler_role
  id = "eventbridge_scheduler_role_spotlake"
}

import {
  to = aws_iam_policy.scheduler_policy
  id = "arn:aws:iam::320674564649:policy/eventbridge_scheduler_policy_spotlake"
}

import {
  to = aws_iam_role_policy_attachment.scheduler_policy_attachment
  id = "eventbridge_scheduler_role_spotlake/arn:aws:iam::320674564649:policy/eventbridge_scheduler_policy_spotlake"
}

import {
  to = aws_cloudwatch_event_rule.collection_schedule
  id = "spotlake-collection-schedule"
}

import {
  to = aws_cloudwatch_event_target.collection_target
  id = "spotlake-collection-schedule/spotlake-collection-target"
}

import {
  to = aws_batch_job_definition.workload_job
  id = "arn:aws:batch:us-west-2:320674564649:job-definition/spotlake-aws-workload-job:5"
}

import {
  to = aws_cloudwatch_event_rule.workload_schedule
  id = "spotlake-workload-schedule"
}

import {
  to = aws_cloudwatch_event_target.workload_schedule
  id = "spotlake-workload-schedule/spotlake-workload-job-target"
}

import {
  to = aws_cloudwatch_event_rule.batch_job_failure[0]
  id = "aws-batch-job-failure"
}

import {
  to = aws_cloudwatch_event_target.batch_failure_to_lambda[0]
  id = "aws-batch-job-failure/BatchFailureNotifierLambda"
}
