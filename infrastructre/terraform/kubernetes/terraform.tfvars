eks_cluster_endpoint = "https://74B70BD1D3E2850A4810A214DE011406.gr7.us-east-1.eks.amazonaws.com"
eks_ca_data          = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURCVENDQWUyZ0F3SUJBZ0lJV2JVeitXZW42dFF3RFFZSktvWklodmNOQVFFTEJRQXdGVEVUTUJFR0ExVUUKQXhNS2EzVmlaWEp1WlhSbGN6QWVGdzB5TlRBek1qWXdPREUxTXpSYUZ3MHpOVEF6TWpRd09ESXdNelJhTUJVeApFekFSQmdOVkJBTVRDbXQxWW1WeWJtVjBaWE13Z2dFaU1BMEdDU3FHU0liM0RRRUJBUVVBQTRJQkR3QXdnZ0VLCkFvSUJBUUR0eFVTUWViSm5JRlF2ZmtOU1hqS2ZkS3RBSjhpMEFPQXE3QU56TExkN3hiR2xBRjhrSlR3aXRIWkUKbUIzZWtwZmZhaEI3cHdPSklrSGxhOE1TL2VQVUNFMUFzbTNmbFJwL2ZKcnd1SWlTTzR6QkdEV1lRVkJMRDNnVwp6Z2xZM1Npb0VFMFVZTTNwY2xpL2NmNDVNZzZ4U2QzUGJoZlJaRHVWOTR5MlR0QkZXcDkrYzhRWmZRMlJLRndKCmRQVUxHL1RraVVmbGwzNkdzbmtMaXBhNjBwaXFSTzZnL0RDNzQ2RUlFblREZFAzYnRYa0ZDN2FNUDRqUlZXRlcKeG5za09ZVXdKRlM3ZGhHVlczZHdiMDg5NW01My82RDlHZ2RjWEE3UDBRSHQ3ZFhWZnk3ZDJ3ZEd5eFFMREljdQpNeUZ2WHBBSGdPNlVxSjdqTmswQnhmTE50V2FwQWdNQkFBR2pXVEJYTUE0R0ExVWREd0VCL3dRRUF3SUNwREFQCkJnTlZIUk1CQWY4RUJUQURBUUgvTUIwR0ExVWREZ1FXQkJUbUd3T3A1djh2OEVYV3RvUGNiMW1aQ2RwTE1qQVYKQmdOVkhSRUVEakFNZ2dwcmRXSmxjbTVsZEdWek1BMEdDU3FHU0liM0RRRUJDd1VBQTRJQkFRQlhmdXRzeWh5dwoyTndiV0VMSDZSandna1lQNDlkeTFmZ1NCQ0pRRG9GdmZZL3lrWkRHcEdYdmdOcm9OOWkyZU5aR1RDVENtMlFwCnRxVFl4Zk1wTlB2SFJkZ2pQelFFTUh6WTZFWm01TFBLb1RSODMvN1F1YUFxd04vMGV0TVJDYzZPek1ZeWtaUFIKODVYaGoraGNMendJYWZNYmhBT0YvUFUyc0VPenFvNWNOZ21LaEtOc2FvZVNsRTBSL0I3STRRbS9Va1NvOGJkKwpoaEw1Zkl2c2lvSDZ0R3RjQjUxTVpzaEpBRTJrb3JnZTNENTNQby84Mk90dVdCZVVWRHg0UUpFWTdxKzVvb2FiCit6QTE1bVpmMVY0dnJQckNwMjNmNWhtTmdWNVZ2TFNEWGhKT3hPTG9GcHhlTXZoRGpvQnNoQmlFbWkveDFjekkKMkJnN1d3c2JXeGdrCi0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K"
cluster_name         = "data-lake"
region               = "us-east-1"

# Copy other variables from the root terraform.tfvars
environment         = "dev"
spark_namespace     = "spark"
s3_bucket_name      = "terraform-20250326081517067100000004"
db_username         = "hiveuser"
db_password         = "hmsuser123"

# From the RDS output
rds_endpoint        = "dev-hms-instance.cqzqmkyse49z.us-east-1.rds.amazonaws.com"
rds_port            = "5432"
rds_database        = "hivemetastore"

# Other settings
aws_auth_method     = "iam_role"
spark_worker_count  = 2
spark_worker_memory = "2G"
spark_worker_cores  = "2"
enable_history_server = true