terraform {
  backend "s3" {
    bucket         = "terraform-state-lzago"
    key            = "web-app/terraform.tfstate"
    region         = "us-east-2"
    dynamodb_table = "terraform-state-locking"
    encrypt        = true
  }

  

  required_providers {
    aws = {
        source = "hashicorp/aws"
        version = "~> 3.0"
    }
  }
}


provider "aws" {
  region = "us-east-2"
}


module "infra_module" {
  source = "../infra_module"
  


#Input Variables 
Env = ""
FeatureName = ""
MicroServiceName = ""
GlueRole = ""
GlueScriptsS3 = ""
GlueDatabaseInputSOR = ""
GlueTableInputSOR = ""
GlueDatabaseOutputSOT = ""
GlueTableOutputSOT = ""
GlueDataInputSORS3 = ""
GlueDataOutputSotS3 = ""
GlueDataOutputStageS3 = ""
ArnKMSS3 = ""
OwnerTeamEmail= ""
TechTeamEmail = ""
DataMeshPixTransacional = ""
MapMigrated = ""
ConnectionAvailabilityZone = ""
ConnectionSecurityGroupIdOne = ""

}