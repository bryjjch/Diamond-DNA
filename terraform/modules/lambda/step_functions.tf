# Daily data-pipeline orchestration: one EventBridge schedule starts a Step Functions
# state machine that runs bronze -> silver -> gold with an explicit dependency chain,
# replacing the old time-staggered per-Lambda cron rules.
#
# Failure semantics: each Lambda returns {"statusCode": 200|207|400, ...} instead of
# raising, so after every stage a Choice state gates on statusCode. Partial results
# (207) proceed to the next stage — silver builds year-to-date and degrades gracefully
# when a bronze source misses a day; only a full error (400) or an unhandled fault
# (timeout / OOM) fails the execution.

# IAM role the state machine assumes to invoke the pipeline Lambdas
resource "aws_iam_role" "data_pipeline_sfn" {
  name = "${var.name_prefix}-data-pipeline-sfn"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "data_pipeline_sfn_invoke" {
  name = "${var.name_prefix}-data-pipeline-sfn-invoke"
  role = aws_iam_role.data_pipeline_sfn.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = [
          aws_lambda_function.bronze_ingestion.arn,
          "${aws_lambda_function.bronze_ingestion.arn}:*",
          aws_lambda_function.silver_feature_build.arn,
          "${aws_lambda_function.silver_feature_build.arn}:*",
          aws_lambda_function.gold_preprocessing.arn,
          "${aws_lambda_function.gold_preprocessing.arn}:*",
        ]
      }
    ]
  })
}

locals {
  # Shared shape for each pipeline stage Task: pass the execution input straight
  # through to the Lambda (a manual StartExecution with start_date/end_date/etc.
  # reaches every handler; the scheduled run passes {} so env/defaults apply),
  # keep only the summary fields from the payload (full per-step sub-results can
  # be verbose; stay far under the 256 KB state limit), retry only Lambda service
  # faults (business failures surface as statusCode, not exceptions), and route
  # unhandled faults (timeout / OOM) to PipelineFailed.
  pipeline_stage_retry = [
    {
      ErrorEquals = [
        "Lambda.ServiceException",
        "Lambda.AWSLambdaException",
        "Lambda.SdkClientException",
        "Lambda.TooManyRequestsException",
      ]
      IntervalSeconds = 30
      MaxAttempts     = 2
      BackoffRate     = 2
    }
  ]
  pipeline_stage_catch = [
    {
      ErrorEquals = ["States.ALL"]
      ResultPath  = "$.fault"
      Next        = "PipelineFailed"
    }
  ]
  pipeline_stage_result_selector = {
    "statusCode.$" = "$.Payload.statusCode"
    "status.$"     = "$.Payload.status"
    "message.$"    = "$.Payload.message"
  }

  data_pipeline_definition = {
    Comment        = "xWAR daily data pipeline: bronze ingestion -> silver build -> gold build"
    TimeoutSeconds = 3600
    StartAt        = "BronzeIngestion"
    States = {
      BronzeIngestion = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.bronze_ingestion.arn
          "Payload.$"  = "$$.Execution.Input"
        }
        ResultSelector = local.pipeline_stage_result_selector
        ResultPath     = "$.bronze"
        TimeoutSeconds = 960
        Retry          = local.pipeline_stage_retry
        Catch          = local.pipeline_stage_catch
        Next           = "CheckBronze"
      }
      CheckBronze = {
        Type = "Choice"
        Choices = [
          {
            Variable        = "$.bronze.statusCode"
            NumericLessThan = 400
            Next            = "SilverBuild"
          }
        ]
        Default = "PipelineFailed"
      }
      SilverBuild = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.silver_feature_build.arn
          "Payload.$"  = "$$.Execution.Input"
        }
        ResultSelector = local.pipeline_stage_result_selector
        ResultPath     = "$.silver"
        TimeoutSeconds = 960
        Retry          = local.pipeline_stage_retry
        Catch          = local.pipeline_stage_catch
        Next           = "CheckSilver"
      }
      CheckSilver = {
        Type = "Choice"
        Choices = [
          {
            Variable        = "$.silver.statusCode"
            NumericLessThan = 400
            Next            = "GoldBuild"
          }
        ]
        Default = "PipelineFailed"
      }
      GoldBuild = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.gold_preprocessing.arn
          "Payload.$"  = "$$.Execution.Input"
        }
        ResultSelector = local.pipeline_stage_result_selector
        ResultPath     = "$.gold"
        TimeoutSeconds = 960
        Retry          = local.pipeline_stage_retry
        Catch          = local.pipeline_stage_catch
        Next           = "CheckGold"
      }
      CheckGold = {
        Type = "Choice"
        Choices = [
          {
            Variable        = "$.gold.statusCode"
            NumericLessThan = 400
            Next            = "PipelineSucceeded"
          }
        ]
        Default = "PipelineFailed"
      }
      PipelineSucceeded = {
        Type = "Succeed"
      }
      PipelineFailed = {
        Type  = "Fail"
        Error = "PipelineStepFailed"
        Cause = "A pipeline stage returned statusCode >= 400 or faulted; see the execution history for the stage payload."
      }
    }
  }
}

# Standard workflow: worst case (3 stages x 900s Lambda timeout + retries) exceeds
# the Express 5-minute cap, and Standard keeps 90-day execution history for free.
resource "aws_sfn_state_machine" "data_pipeline" {
  name     = "${var.name_prefix}-data-pipeline"
  role_arn = aws_iam_role.data_pipeline_sfn.arn
  type     = "STANDARD"

  definition = jsonencode(local.data_pipeline_definition)

  tags = var.tags
}

# IAM role EventBridge assumes to start pipeline executions
resource "aws_iam_role" "data_pipeline_events" {
  name = "${var.name_prefix}-data-pipeline-events"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "data_pipeline_events_start" {
  name = "${var.name_prefix}-data-pipeline-events-start"
  role = aws_iam_role.data_pipeline_events.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = aws_sfn_state_machine.data_pipeline.arn
      }
    ]
  })
}

# Single daily schedule for the whole pipeline (22:00 UTC so the upstream APIs have
# finished publishing the prior day's data; each handler still targets yesterday UTC).
resource "aws_cloudwatch_event_rule" "data_pipeline" {
  name                = "${var.name_prefix}-data-pipeline-schedule"
  description         = "Trigger the daily bronze -> silver -> gold pipeline state machine (processes the prior UTC date)"
  schedule_expression = var.pipeline_schedule_expression
  tags                = var.tags
}

resource "aws_cloudwatch_event_target" "data_pipeline" {
  rule      = aws_cloudwatch_event_rule.data_pipeline.name
  target_id = "DataPipelineStateMachine"
  arn       = aws_sfn_state_machine.data_pipeline.arn
  role_arn  = aws_iam_role.data_pipeline_events.arn

  # Clean event so every handler falls back to env vars / yesterday-UTC defaults
  # (the raw EventBridge envelope would otherwise be passed as the Lambda payload).
  input = jsonencode({})
}
