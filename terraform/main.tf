# resource "aws_instance" "de-ec2" {
#   ami           = var.ec2_ami
#   instance_type = "t2.micro"

#   ebs_block_device {
#     device_name = "/dev/sda1"
#     volume_size = 10
#   }

#   tags = {
#     Name = "EC2forDEprojects"
#   }

# }

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