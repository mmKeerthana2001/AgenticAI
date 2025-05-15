import boto3
import logging
import base64
import os
from semantic_kernel.functions import kernel_function

logger = logging.getLogger(__name__)

class AWSPlugin:
    def __init__(self):
        self.session = boto3.Session()

    @kernel_function(
        description="Create an S3 bucket",
        name="create_bucket"
    )
    async def create_bucket(self, bucket_name: str, region: str = "us-east-1", acl: str = "private") -> dict:
        try:
            s3_client = self.session.client('s3', region_name=region)
            create_params = {"Bucket": bucket_name, "ACL": acl}
            if region != "us-east-1":
                create_params["CreateBucketConfiguration"] = {"LocationConstraint": region}
            s3_client.create_bucket(**create_params)
            logger.info(f"Created S3 bucket {bucket_name} in {region} with ACL {acl}")
            return {"success": True, "message": f"S3 bucket {bucket_name} created successfully"}
        except Exception as e:
            logger.error(f"Error creating S3 bucket {bucket_name}: {str(e)}")
            return {"success": False, "message": f"Failed to create S3 bucket: {str(e)}"}

    @kernel_function(
        description="Delete an S3 bucket",
        name="delete_bucket"
    )
    async def delete_bucket(self, bucket_name: str, region: str = "us-east-1") -> dict:
        try:
            s3_client = self.session.client('s3', region_name=region)
            response = s3_client.list_objects_v2(Bucket=bucket_name)
            if 'Contents' in response:
                s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': [{'Key': obj['Key']} for obj in response['Contents']]})
            s3_client.delete_bucket(Bucket=bucket_name)
            logger.info(f"Deleted S3 bucket {bucket_name} in {region}")
            return {"success": True, "message": f"S3 bucket {bucket_name} deleted successfully"}
        except Exception as e:
            logger.error(f"Error deleting S3 bucket {bucket_name}: {str(e)}")
            return {"success": False, "message": f"Failed to delete S3 bucket: {str(e)}"}

    @kernel_function(
        description="Launch an EC2 instance with user data to clone a repository and run a script",
        name="launch_instance"
    )
    async def launch_instance(self, instance_type: str, ami_id: str, region: str = "us-east-1", repo_name: str = None, script_name: str = None, github_token: str = None, source_bucket: str = None, destination_bucket: str = None) -> dict:
        """
        Launch an EC2 instance with user data to clone a repository and run a script.
        Args:
            instance_type (str): EC2 instance type (e.g., t3.micro).
            ami_id (str): AMI ID for the instance.
            region (str): AWS region (default: us-east-1).
            repo_name (str): GitHub repository name to clone (optional).
            script_name (str): Script to run on the instance (optional).
            github_token (str): GitHub token for private repository access (optional, defaults to GITHUB_TOKEN from env).
            source_bucket (str): Source S3 bucket for the script (optional).
            destination_bucket (str): Destination S3 bucket for output (optional).
        Returns:
            dict: {success: bool, message: str, instance_id: str, logs: str}.
        """
        try:
            ec2_client = self.session.client('ec2', region_name=region)
            user_data = None
            logs = "No logs captured"
            if repo_name and script_name:
                github_token = github_token or os.getenv("GITHUB_TOKEN")
                if not github_token:
                    raise ValueError("GITHUB_TOKEN must be provided in arguments or .env for private repository access")
                github_username = os.getenv("GITHUB_USERNAME", "lakshmeesh12")
                repo_url = f"https://{github_token}@github.com/{github_username}/{repo_name}.git"
                user_data_script = f"""#!/bin/bash
set -e
LOG_FILE=/var/log/user-data.log
echo "Starting user data script" >> $LOG_FILE
sudo yum update -y >> $LOG_FILE 2>&1
echo "Installing git and aws-cli" >> $LOG_FILE
sudo yum install -y git aws-cli >> $LOG_FILE 2>&1
echo "Cloning repository {repo_url}" >> $LOG_FILE
git clone {repo_url} /home/ec2-user/repo >> $LOG_FILE 2>&1
if [ $? -ne 0 ]; then
    echo "Failed to clone repository" >> $LOG_FILE
    exit 1
fi
cd /home/ec2-user/repo
echo "Setting executable permissions for {script_name}" >> $LOG_FILE
chmod +x {script_name} >> $LOG_FILE 2>&1
echo "Executing {script_name}" >> $LOG_FILE
./{script_name} >> $LOG_FILE 2>&1
if [ $? -ne 0 ]; then
    echo "Failed to execute {script_name}" >> $LOG_FILE
    exit 1
fi
echo "User data script completed successfully" >> $LOG_FILE
aws s3 cp $LOG_FILE s3://{destination_bucket}/user-data.log >> $LOG_FILE 2>&1
"""
                user_data = base64.b64encode(user_data_script.encode()).decode()

            run_instances_params = {
                "ImageId": ami_id,
                "InstanceType": instance_type,
                "MinCount": 1,
                "MaxCount": 1,
                "UserData": user_data if user_data else ""
            }

            # Add KeyName only if EC2_KEY_PAIR is specified and exists
            key_pair = os.getenv("EC2_KEY_PAIR")
            if key_pair:
                # Verify key pair exists
                try:
                    ec2_client.describe_key_pairs(KeyNames=[key_pair])
                    run_instances_params["KeyName"] = key_pair
                except ec2_client.exceptions.ClientError as e:
                    raise ValueError(f"Key pair '{key_pair}' does not exist in region {region}: {str(e)}")
            else:
                logger.warning("No EC2_KEY_PAIR specified in .env. Launching instance without SSH key pair.")

            # Include IamInstanceProfile for S3 access
            if source_bucket or destination_bucket:
                iam_role_arn = os.getenv("EC2_IAM_ROLE_ARN", "arn:aws:iam::296062547225:instance-profile/EC2S3AccessRole")
                run_instances_params["IamInstanceProfile"] = {"Arn": iam_role_arn}

            response = ec2_client.run_instances(**run_instances_params)
            instance_id = response['Instances'][0]['InstanceId']
            logger.info(f"Launched EC2 instance {instance_id} with type {instance_type} in {region}")
            return {
                "success": True,
                "message": f"EC2 instance {instance_id} launched successfully",
                "instance_id": instance_id,
                "logs": logs  # Logs will be updated later if retrieved
            }
        except Exception as e:
            logger.error(f"Error launching EC2 instance: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to launch EC2 instance: {str(e)}",
                "instance_id": None,
                "logs": logs
            }

    @kernel_function(
        description="Terminate an EC2 instance",
        name="terminate_instance"
    )
    async def terminate_instance(self, instance_id: str, region: str = "us-east-1") -> dict:
        try:
            ec2_client = self.session.client('ec2', region_name=region)
            ec2_client.terminate_instances(InstanceIds=[instance_id])
            logger.info(f"Terminated EC2 instance {instance_id} in {region}")
            return {"success": True, "message": f"EC2 instance {instance_id} terminated successfully"}
        except Exception as e:
            logger.error(f"Error terminating EC2 instance {instance_id}: {str(e)}")
            return {"success": False, "message": f"Failed to terminate EC2 instance: {str(e)}"}

    @kernel_function(
        description="Add an IAM user",
        name="add_user"
    )
    async def add_user(self, username: str) -> dict:
        try:
            iam_client = self.session.client('iam')
            iam_client.create_user(UserName=username)
            logger.info(f"Created IAM user {username}")
            return {"success": True, "message": f"IAM user {username} created successfully"}
        except Exception as e:
            logger.error(f"Error creating IAM user {username}: {str(e)}")
            return {"success": False, "message": f"Failed to create IAM user: {str(e)}"}

    @kernel_function(
        description="Remove an IAM user",
        name="remove_user"
    )
    async def remove_user(self, username: str) -> dict:
        try:
            iam_client = self.session.client('iam')
            iam_client.delete_user(UserName=username)
            logger.info(f"Removed IAM user {username}")
            return {"success": True, "message": f"IAM user {username} removed successfully"}
        except Exception as e:
            logger.error(f"Error removing IAM user {username}: {str(e)}")
            return {"success": False, "message": f"Failed to remove IAM user: {str(e)}"}

    @kernel_function(
        description="Add permission to an IAM user",
        name="add_user_permission"
    )
    async def add_user_permission(self, username: str, permission: str) -> dict:
        try:
            iam_client = self.session.client('iam')
            iam_client.attach_user_policy(
                UserName=username,
                PolicyArn=permission
            )
            logger.info(f"Added permission {permission} to IAM user {username}")
            return {"success": True, "message": f"Permission {permission} added to IAM user {username}"}
        except Exception as e:
            logger.error(f"Error adding permission to IAM user {username}: {str(e)}")
            return {"success": False, "message": f"Failed to add permission: {str(e)}"}

    @kernel_function(
        description="Remove permission from an IAM user",
        name="remove_user_permission"
    )
    async def remove_user_permission(self, username: str, permission: str) -> dict:
        try:
            iam_client = self.session.client('iam')
            iam_client.detach_user_policy(
                UserName=username,
                PolicyArn=permission
            )
            logger.info(f"Removed permission {permission} from IAM user {username}")
            return {"success": True, "message": f"Permission {permission} removed from IAM user {username}"}
        except Exception as e:
            logger.error(f"Error removing permission from IAM user {username}: {str(e)}")
            return {"success": False, "message": f"Failed to remove permission: {str(e)}"}