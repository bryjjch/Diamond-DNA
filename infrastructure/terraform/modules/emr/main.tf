terraform {
  required_version = ">= 1.0"
}

resource "aws_emr_cluster" "main" {
  name          = var.cluster_name
  release_label = var.release_label
  applications  = var.applications

  ec2_attributes {
    subnet_id                         = var.subnet_id
    emr_managed_master_security_group = aws_security_group.emr_master.id
    emr_managed_slave_security_group  = aws_security_group.emr_slave.id
    instance_profile                  = aws_iam_instance_profile.emr_instance_profile.arn
  }

  master_instance_group {
    instance_type  = var.master_instance_type
    instance_count = 1
  }

  core_instance_group {
    instance_type  = var.core_instance_type
    instance_count = var.core_instance_count
  }

  service_role     = aws_iam_role.emr_service.arn
  autoscaling_role = aws_iam_role.emr_autoscaling.arn

  # Auto-terminate for transient cluster
  auto_termination_policy {
    idle_timeout = var.auto_termination_idle_timeout
  }

  # Bootstrap actions
  dynamic "bootstrap_action" {
    for_each = var.bootstrap_actions
    content {
      path = bootstrap_action.value.path
      name = bootstrap_action.value.name
      args = bootstrap_action.value.args
    }
  }

  # Log URI for CloudWatch
  log_uri = var.log_uri

  tags = merge(
    var.tags,
    {
      Name = var.cluster_name
    }
  )
}

# Security Group for EMR Master
resource "aws_security_group" "emr_master" {
  name_prefix = "${var.cluster_name}-emr-master-"
  vpc_id      = var.vpc_id
  description = "Security group for EMR master node"

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(
    var.tags,
    {
      Name = "${var.cluster_name}-emr-master-sg"
    }
  )
}

# Security Group for EMR Slave
resource "aws_security_group" "emr_slave" {
  name_prefix = "${var.cluster_name}-emr-slave-"
  vpc_id      = var.vpc_id
  description = "Security group for EMR slave/core nodes"

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(
    var.tags,
    {
      Name = "${var.cluster_name}-emr-slave-sg"
    }
  )
}

# Allow communication between master and slave
resource "aws_security_group_rule" "master_to_slave" {
  type                     = "egress"
  from_port                = 0
  to_port                  = 65535
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.emr_slave.id
  security_group_id        = aws_security_group.emr_master.id
}

resource "aws_security_group_rule" "slave_to_master" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 65535
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.emr_master.id
  security_group_id        = aws_security_group.emr_slave.id
}
