terraform {
  required_version = ">= 1.0"
}

# SageMaker Model
resource "aws_sagemaker_model" "main" {
  name               = "${var.endpoint_name}-model"
  execution_role_arn = aws_iam_role.sagemaker.arn

  primary_container {
    image         = var.model_image
    model_data_url = var.model_data_url != null ? var.model_data_url : "s3://${var.model_artifacts_bucket_name}/${var.model_artifacts_path}"

    environment = var.model_environment_variables
  }

  vpc_config {
    subnets            = var.subnet_ids
    security_group_ids = var.security_group_ids
  }

  tags = merge(
    var.tags,
    {
      Name = "${var.endpoint_name}-model"
    }
  )
}

# SageMaker Endpoint Configuration
resource "aws_sagemaker_endpoint_configuration" "main" {
  name = "${var.endpoint_name}-config"

  production_variants {
    variant_name           = "primary"
    model_name             = aws_sagemaker_model.main.name
    instance_type          = var.instance_type
    initial_instance_count = var.initial_instance_count
  }

  kms_key_id = var.kms_key_id

  tags = merge(
    var.tags,
    {
      Name = "${var.endpoint_name}-config"
    }
  )
}

# SageMaker Endpoint
resource "aws_sagemaker_endpoint" "main" {
  name                 = var.endpoint_name
  endpoint_config_name = aws_sagemaker_endpoint_configuration.main.name

  tags = merge(
    var.tags,
    {
      Name = var.endpoint_name
    }
  )
}
