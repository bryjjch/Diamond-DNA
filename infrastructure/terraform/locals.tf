locals {
  availability_zones = var.availability_zones

  common_tags = merge(
    var.tags,
    {
      Environment = var.environment
      Project     = "deeproster"
    }
  )
}