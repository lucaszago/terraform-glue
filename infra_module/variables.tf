variable "Env" {
    description = "Environment"
    type = string  
}

variable "FeatureName" {
    type = string
}

variable "MicroServiceName" {
    type = string 
  
}

variable "GlueRole" {
    description = "Nome da role que o glue irá utilizar, deve possuir a policy GlueServiceRole"
    type = string
  
}

variable "GlueScriptsS3" {
    description = "Nome do bucket s3 onde os scripts glues estarão armazenados"
    type = string
}

variable "GlueDatabaseInputSOR" {
    description = "nome do database na camada SOR"
    type = string
}

variable "GlueTableInputSOR" {
    description = "nome da tabela na camada sor"
    type = string
  
}

variable "GlueDatabaseOutputSOT" {
  description = "nome do database na camada sot"
  type = string
}

variable "GlueTableOutputSOT" {
    description = "nome da tabela na camada SOT"
    type = string
}

variable "GlueDataInputSORS3" {
    description = "Nome bucket s3 da sor onde os dados do glue estão"
    type = string
}

variable "GlueDataOutputSotS3" {
    description = "bucket s3 da fato onde os dados glue estão"
    type = string

}

variable "GlueDataOutputStageS3" {
    description = "Nome do bucket s3 da stage da sot onde os dados do Glue estão."
    type = string   
}

variable "ArnKMSS3" {
    description = "arn de criptografia do kms utilizado no bucket s3"
    type = string
}


variable "OwnerTeamEmail" {
    description = "email owner team"
    type = string
}

variable "TechTeamEmail" {
    description = "email tech team"
    type = string
}

variable "DataMeshPixTransacional" {
    description = "Data Mesh Pix Transacional"
    type = string
    default = "datameshpixtransacional"
}

variable "MapMigrated" {
    description = "Valor da TAG map-migrated"
    type = string
    default = "mig44463"
}

variable "ConnectionAvailabilityZone" {
    description = " Availability Zone utilizado pelo Glue Connection"
    type = string
}

variable "ConnectionSecurityGroupIdOne" {
    description = "Security Group Id utilizado pelo Glue Connection."
    type = string
  
}
