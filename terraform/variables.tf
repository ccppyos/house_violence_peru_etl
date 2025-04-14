variable "region" {
  default = "us-east-1"
}


variable "ec2_ami" {
  default = "ami-071226ecf16aa7d96"
}

variable "s3_bucket_name" {
  default = "data-camp-bucket-crp"
}

variable "cluster_id" {
  default = "redshift-cluster-0"
}

variable "node_type" {
  default = "dc2.large"
}

variable "cluster_type" {
  default = "single-node"
}

variable "db_credentials_uname" {
  description = "Redshift database user"
  type        = string
}

variable "db_credentials_pwd" {
  description = "Redshift database password"
  type        = string
  sensitive   = true
}

