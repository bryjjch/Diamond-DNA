output "job_queue_arn" {
  description = "ARN of the Batch job queue"
  value       = aws_batch_job_queue.backfill.arn
}

output "job_queue_name" {
  description = "Name of the Batch job queue"
  value       = aws_batch_job_queue.backfill.name
}

output "job_definition_arn" {
  description = "ARN of the Batch job definition"
  value       = aws_batch_job_definition.backfill.arn
}

output "job_definition_name" {
  description = "Name of the Batch job definition"
  value       = aws_batch_job_definition.backfill.name
}

output "compute_environment_arn" {
  description = "ARN of the Batch compute environment"
  value       = aws_batch_compute_environment.backfill.arn
}

output "compute_environment_name" {
  description = "Name of the Batch compute environment"
  value       = aws_batch_compute_environment.backfill.name
}
