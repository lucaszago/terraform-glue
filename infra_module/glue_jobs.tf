resource "aws_glue_job" "name" {
    name = "${var.FeatureName}-${var.MicroServiceName}-${var.Env}-datamesh-sot-one-big-table"
    role_arn = var.GlueRole

    command {
        script_location = "s3://${var.GlueScriptsS3}/app/main_data_mesh_sot_one_big_table.py"
        python_version = "3"
      
    }

    default_arguments = {
        "--job-bookmark-option" = "job-bookmark-disable"
    }
  
}

resource "aws_glue_trigger" "data_mesh_sot_one_big_table" {
    name = "${var.FeatureName}-${var.MicroServiceName}-${var.Env}-trigger-datamesh-sot-one-big-table"
    type = "SCHEDULED"
    schedule = "cron(15 * * * *)"
    start_on_creation = true

    actions {
        job_name = aws_glue_job.data_mesh_sot_one_big_table
      
    }
  
}