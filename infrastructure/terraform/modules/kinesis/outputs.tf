output "stream_name" {
  description = "Name of the Kinesis stream"
  value       = aws_kinesis_stream.main.name
}

output "stream_arn" {
  description = "ARN of the Kinesis stream"
  value       = aws_kinesis_stream.main.arn
}

output "stream_id" {
  description = "ID of the Kinesis stream"
  value       = aws_kinesis_stream.main.id
}
