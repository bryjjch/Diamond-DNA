output "cluster_id" {
  description = "ID of the EMR cluster"
  value       = aws_emr_cluster.main.id
}

output "cluster_name" {
  description = "Name of the EMR cluster"
  value       = aws_emr_cluster.main.name
}

output "master_public_dns" {
  description = "Public DNS name of the master node"
  value       = aws_emr_cluster.main.master_public_dns
}

output "master_security_group_id" {
  description = "ID of the master security group"
  value       = aws_security_group.emr_master.id
}

output "slave_security_group_id" {
  description = "ID of the slave/core security group"
  value       = aws_security_group.emr_slave.id
}
