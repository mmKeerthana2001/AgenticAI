import os
import logging
import requests
from dotenv import load_dotenv
from semantic_kernel.functions import kernel_function
import base64

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class GitPlugin:
    def __init__(self):
        self.client = GitClient()

    @kernel_function(
        description="Grant read (pull) or write (push) access to a GitHub repository.",
        name="grant_repo_access"
    )
    async def grant_repo_access(self, repo_name: str, github_username: str, access_type: str) -> dict:
        """
        Grant access to a GitHub repository.
        Args:
            repo_name (str): Repository name.
            github_username (str): GitHub username.
            access_type (str): Access type (pull or push).
        Returns:
            dict: {success: bool, message: str}.
        """
        success, message = self.client.grant_repo_access(repo_name, github_username, access_type)
        return {"success": success, "message": message}

    @kernel_function(
        description="Revoke access for a user from a GitHub repository.",
        name="revoke_repo_access"
    )
    async def revoke_repo_access(self, repo_name: str, github_username: str) -> dict:
        """
        Revoke access to a GitHub repository.
        Args:
            repo_name (str): Repository name.
            github_username (str): GitHub username.
        Returns:
            dict: {success: bool, message: str}.
        """
        success, message = self.client.revoke_repo_access(repo_name, github_username)
        return {"success": success, "message": message}
    
    @kernel_function(
        description="Create a private GitHub repository.",
        name="create_repo"
    )
    async def create_repo(self, repo_name: str) -> dict:
        """
        Create a private GitHub repository.
        Args:
            repo_name (str): Repository name.
        Returns:
            dict: {success: bool, message: str}.
        """
        success, message = self.client.create_repo(repo_name)
        return {"success": success, "message": message}

    @kernel_function(
        description="Commit a file to a GitHub repository.",
        name="commit_file"
    )
    async def commit_file(self, repo_name: str, file_name: str, file_content: str) -> dict:
        """
        Commit a file to a GitHub repository.
        Args:
            repo_name (str): Repository name.
            file_name (str): Name of the file to commit.
            file_content (str): Content of the file.
        Returns:
            dict: {success: bool, message: str}.
        """
        success, message = self.client.commit_file(repo_name, file_name, file_content)
        return {"success": success, "message": message}

    @kernel_function(
        description="Delete a GitHub repository.",
        name="delete_repo"
    )
    async def delete_repo(self, repo_name: str) -> dict:
        """
        Delete a GitHub repository.
        Args:
            repo_name (str): Repository name.
        Returns:
            dict: {success: bool, message: str}.
        """
        success, message = self.client.delete_repo(repo_name)
        return {"success": success, "message": message}

class GitClient:
    def __init__(self):
        self.github_token = os.getenv("GITHUB_TOKEN")
        self.github_org = os.getenv("GITHUB_ORG", "LakshmeeshOrg")
        self.base_url = "https://api.github.com"
        self.headers = {
            "Authorization": f"Bearer {self.github_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28"
        }
        self.supported_apps = ["github"]
        logger.info("Initialized GitClient for GitHub integration")

    def is_supported_app(self, app_name):
        """Check if the third-party app is supported."""
        return app_name.lower() in self.supported_apps

    def grant_repo_access(self, repo_name, github_username, access_type):
        """Grant read (pull) or write (push) access to a GitHub repository."""
        try:
            if access_type not in ["pull", "push"]:
                logger.error(f"Invalid access type: {access_type}. Must be 'pull' or 'push'.")
                return False, f"Invalid access type: {access_type}"

            repo_url = f"{self.base_url}/repos/{self.github_org}/{repo_name}"
            repo_response = requests.get(repo_url, headers=self.headers)
            if repo_response.status_code != 200:
                logger.error(f"Repository {repo_name} not found or inaccessible: {repo_response.text}")
                return False, f"Repository {repo_name} not found"

            collab_url = f"{self.base_url}/repos/{self.github_org}/{repo_name}/collaborators/{github_username}"
            payload = {"permission": access_type}
            response = requests.put(collab_url, headers=self.headers, json=payload)

            if response.status_code == 201 or response.status_code == 204:
                logger.info(f"Granted {access_type} access to {github_username} for repo {repo_name}")
                return True, f"{access_type.capitalize()} access granted to {github_username} for {repo_name}"
            else:
                logger.error(f"Failed to grant access: {response.text}")
                return False, f"Failed to grant access: {response.text}"
        except Exception as e:
            logger.error(f"Error granting repo access: {str(e)}")
            return False, f"Error granting access: {str(e)}"

    def revoke_repo_access(self, repo_name, github_username):
        """Revoke access for a user from a GitHub repository."""
        try:
            repo_url = f"{self.base_url}/repos/{self.github_org}/{repo_name}"
            repo_response = requests.get(repo_url, headers=self.headers)
            if repo_response.status_code != 200:
                logger.error(f"Repository {repo_name} not found or inaccessible: {repo_response.text}")
                return False, f"Repository {repo_name} not found"

            collab_url = f"{self.base_url}/repos/{self.github_org}/{repo_name}/collaborators/{github_username}"
            response = requests.delete(collab_url, headers=self.headers)

            if response.status_code == 204:
                logger.info(f"Revoked access for {github_username} from repo {repo_name}")
                return True, f"Access revoked for {github_username} from {repo_name}"
            else:
                logger.error(f"Failed to revoke access: {response.text}")
                return False, f"Failed to revoke access: {response.text}"
        except Exception as e:
            logger.error(f"Error revoking repo access: {str(e)}")
            return False, f"Error revoking access: {str(e)}"
        
    def create_repo(self, repo_name):
        """Create a private GitHub repository."""
        try:
            url = f"{self.base_url}/user/repos"
            payload = {
                "name": repo_name,
                "private": True,
                "auto_init": False
            }
            response = requests.post(url, headers=self.headers, json=payload)
            if response.status_code == 201:
                logger.info(f"Created private repository {repo_name}")
                return True, f"Private repository {repo_name} created successfully"
            else:
                logger.error(f"Failed to create repository {repo_name}: {response.text}")
                return False, f"Failed to create repository: {response.text}"
        except Exception as e:
            logger.error(f"Error creating repository {repo_name}: {str(e)}")
            return False, f"Error creating repository: {str(e)}"

    def commit_file(self, repo_name, file_name, file_content):
        """Commit a file to a GitHub repository."""
        try:
            # Check if repository exists
            repo_url = f"{self.base_url}/repos/{self.github_org}/{repo_name}"
            repo_response = requests.get(repo_url, headers=self.headers)
            if repo_response.status_code != 200:
                logger.error(f"Repository {repo_name} not found: {repo_response.text}")
                return False, f"Repository {repo_name} not found"

            # Encode file content to base64
            encoded_content = base64.b64encode(file_content.encode()).decode()

            # Commit the file
            commit_url = f"{self.base_url}/repos/{self.github_org}/{repo_name}/contents/{file_name}"
            payload = {
                "message": f"Add {file_name}",
                "content": encoded_content,
                "branch": "main"
            }
            response = requests.put(commit_url, headers=self.headers, json=payload)
            if response.status_code == 201:
                logger.info(f"Committed {file_name} to repository {repo_name}")
                return True, f"File {file_name} committed to {repo_name}"
            else:
                logger.error(f"Failed to commit {file_name}: {response.text}")
                return False, f"Failed to commit file: {response.text}"
        except Exception as e:
            logger.error(f"Error committing file {file_name} to {repo_name}: {str(e)}")
            return False, f"Error committing file: {str(e)}"

    def delete_repo(self, repo_name):
        """Delete a GitHub repository."""
        try:
            repo_url = f"{self.base_url}/repos/{self.github_org}/{repo_name}"
            response = requests.delete(repo_url, headers=self.headers)
            if response.status_code == 204:
                logger.info(f"Deleted repository {repo_name}")
                return True, f"Repository {repo_name} deleted successfully"
            else:
                logger.error(f"Failed to delete repository {repo_name}: {response.text}")
                return False, f"Failed to delete repository: {response.text}"
        except Exception as e:
            logger.error(f"Error deleting repository {repo_name}: {str(e)}")
            return False, f"Error deleting repository: {str(e)}"