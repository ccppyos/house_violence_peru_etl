resource "aws_s3_bucket" "de-s3" {
  bucket = var.s3_bucket_name

  tags = {
    Name        = "S3forDEprojects "
    Environment = "Dev"
  }
}

resource "aws_redshift_cluster" "de-redshift" {
  cluster_identifier  = var.cluster_id
  database_name       = "dev"
  master_username     = var.db_credentials_uname
  master_password     = var.db_credentials_pwd
  node_type           = var.node_type
  cluster_type        = var.cluster_type
  publicly_accessible = true
  skip_final_snapshot = true
}