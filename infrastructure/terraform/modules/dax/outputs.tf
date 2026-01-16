output "cluster_id" {
  description = "ID of the DAX cluster"
  value       = aws_dax_cluster.main.cluster_id
}

output "cluster_endpoint" {
  description = "Configuration endpoint of the DAX cluster"
  value       = aws_dax_cluster.main.cluster_address
}

output "cluster_arn" {
  description = "ARN of the DAX cluster"
  value       = aws_dax_cluster.main.arn
}

output "security_group_id" {
  description = "ID of the DAX security group"
  value       = aws_security_group.dax.id
}
