import os
import requests
import base64
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration
WORKSPACE_CONFIG = {
    "AWS": {
        "url": os.getenv("AWS_WORKSPACE_URL"),
        "token": os.getenv("AWS_ACCESS_TOKEN"),
    },
    "AZURE": {
        "url": os.getenv("AZURE_WORKSPACE_URL"),
        "token": os.getenv("AZURE_ACCESS_TOKEN"),
    },
    "GCP": {
        "url": os.getenv("GCP_WORKSPACE_URL"),
        "token": os.getenv("GCP_ACCESS_TOKEN"),
    },
}

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO_OWNER = os.getenv("GITHUB_REPO_OWNER")
GITHUB_REPO_NAME = os.getenv("GITHUB_REPO_NAME")


def get_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }


def get_workspace_config(cloud_provider):
    return WORKSPACE_CONFIG.get(cloud_provider.upper(), None)


def fetch_notebook_from_github(repo_owner, repo_name, notebook_path, github_token):
    """
    Fetch a notebook from GitHub using the GitHub API.
    """
    api_endpoint = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{notebook_path}"
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }
    try:
        response = requests.get(api_endpoint, headers=headers)
        response.raise_for_status()
        content = response.json().get("content")
        if content:
            # Decode base64 content
            return base64.b64decode(content).decode("utf-8")
        else:
            print(f"Notebook {notebook_path} not found in GitHub repository.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching notebook {notebook_path} from GitHub: {e}")
        return None


def import_notebook(workspace_url, access_token, content, notebook_name, workspace_dir):
    """
    Import a notebook into a Databricks workspace.
    """
    api_endpoint = f"{workspace_url}/api/2.0/workspace/import"
    notebook_path = f"{workspace_dir}/{notebook_name}"
    data = {
        "path": notebook_path,
        "format": "SOURCE",
        "language": "PYTHON",
        "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
        "overwrite": True
    }
    try:
        response = requests.post(api_endpoint, headers=get_headers(access_token), json=data)
        response.raise_for_status()
        print(f"Notebook imported successfully to {notebook_path} in {workspace_url}")
        return notebook_path
    except requests.exceptions.RequestException as e:
        print(f"Error importing notebook to {notebook_path} in {workspace_url}: {e}")
        return None


def get_notebook_id(workspace_url, access_token, notebook_path):
    """
    Fetch the notebook ID using the get-status API.
    """
    api_endpoint = f"{workspace_url}/api/2.0/workspace/get-status"
    params = {"path": notebook_path}
    try:
        response = requests.get(api_endpoint, headers=get_headers(access_token), params=params)
        response.raise_for_status()
        print(f"Notebook details: {response.json()}")  # Debugging log
        return response.json().get("object_id")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching notebook ID for {notebook_path} in {workspace_url}: {e}")
        return None


def get_object_status(workspace_url, access_token, path):
    """
    Fetch the status of an object (notebook or directory) in a Databricks workspace.
    """
    api_endpoint = f"{workspace_url}/api/2.0/workspace/get-status"
    params = {"path": path}
    try:
        response = requests.get(api_endpoint, headers=get_headers(access_token), params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching object status for {path} in {workspace_url}: {e}")
        return None


def get_permissions(workspace_url, access_token, path):
    """
    Fetch permissions for a notebook or directory in a Databricks workspace.
    """
    object_status = get_object_status(workspace_url, access_token, path)
    if not object_status:
        return None

    object_id = object_status.get("object_id")
    object_type = object_status.get("object_type")

    if object_type == "NOTEBOOK":
        api_endpoint = f"{workspace_url}/api/2.0/permissions/notebooks/{object_id}"
    elif object_type == "DIRECTORY":
        api_endpoint = f"{workspace_url}/api/2.0/permissions/directories/{object_id}"
    else:
        print(f"Unsupported object type: {object_type}")
        return None

    try:
        response = requests.get(api_endpoint, headers=get_headers(access_token))
        response.raise_for_status()
        permissions_list = response.json().get("access_control_list", [])
        
        # Convert the list of permissions into a dictionary
        permissions_dict = {}
        for permission in permissions_list:
            user_name = permission.get("user_name")
            permission_level = permission.get("permission_level")
            if user_name and permission_level:
                permissions_dict[user_name] = permission_level
        
        return permissions_dict
    except requests.exceptions.RequestException as e:
        print(f"Error fetching permissions for {path} in {workspace_url}: {e}")
        return {}


def grant_permissions(workspace_url, access_token, path, email, permission_level, cluster_id=None):
    """
    Grant permissions to a user for a notebook or directory in a Databricks workspace.
    """
    object_status = get_object_status(workspace_url, access_token, path)
    if not object_status:
        print(f"Skipping permissions for {path} - object status not found.")
        return False

    object_id = object_status.get("object_id")
    object_type = object_status.get("object_type")

    if object_type == "NOTEBOOK":
        api_endpoint = f"{workspace_url}/api/2.0/permissions/notebooks/{object_id}"
    elif object_type == "DIRECTORY":
        api_endpoint = f"{workspace_url}/api/2.0/permissions/directories/{object_id}"
    else:
        print(f"Skipping permissions for {path} - unsupported object type: {object_type}")
        return False

    data = {
        "access_control_list": [
            {
                "user_name": email,
                "permission_level": permission_level
            }
        ]
    }

    if cluster_id:
        data["access_control_list"].append({
            "user_name": email,
            "permission_level": "CAN_ATTACH_TO",
            "cluster_id": cluster_id
        })

    try:
        response = requests.patch(api_endpoint, headers=get_headers(access_token), json=data)
        response.raise_for_status()
        print(f"Granted {permission_level} permissions to {email} for {path}")
        if cluster_id:
            print(f"Granted CAN_ATTACH_TO permissions to {email} for cluster {cluster_id}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error granting permissions for {path}: {e}")
        return False


def sync_notebooks_and_permissions(source_cloud, target_cloud, git_url=None, cluster_id=None):
    print("Starting notebook synchronization and permission sync...")

    # Get workspace configurations
    source_config = get_workspace_config(source_cloud)
    target_config = get_workspace_config(target_cloud)

    if not source_config or not target_config:
        print("Invalid source or target cloud provider.")
        return

    # Fetch the notebook from GitHub
    notebook_name = "demon_slayer_notebook.py"  # Replace with the name of your notebook in the GitHub repo
    notebook_content = fetch_notebook_from_github(GITHUB_REPO_OWNER, GITHUB_REPO_NAME, notebook_name, GITHUB_TOKEN)
    if not notebook_content:
        print("Failed to fetch notebook from GitHub. Exiting.")
        return

    # Define the workspace directory where the notebook will be imported
    workspace_dir = "/Workspace/Users/adhyan.mishra@digivatelabs.com"

    # Import the notebook into the source workspace
    print(f"Importing notebook to {source_cloud} workspace...")
    source_notebook_path = import_notebook(source_config["url"], source_config["token"], notebook_content, notebook_name, workspace_dir)
    if not source_notebook_path:
        print("Failed to import notebook into source workspace. Exiting.")
        return

    # Import the notebook into the target workspace
    print(f"Importing notebook to {target_cloud} workspace...")
    target_notebook_path = import_notebook(target_config["url"], target_config["token"], notebook_content, notebook_name, workspace_dir)
    if not target_notebook_path:
        print("Failed to import notebook into target workspace. Exiting.")
        return

    # Fetch the notebook ID in the source workspace
    print(f"Fetching notebook ID in {source_cloud} workspace...")
    source_notebook_id = get_notebook_id(source_config["url"], source_config["token"], source_notebook_path)
    if not source_notebook_id:
        print(f"Failed to fetch notebook ID for {source_notebook_path} in {source_cloud} workspace.")
        return

    # Fetch permissions from the source workspace
    print(f"Fetching permissions from {source_cloud} workspace...")
    source_permissions = get_permissions(source_config["url"], source_config["token"], source_notebook_path)
    if not source_permissions:
        print(f"No permissions found for notebook ID {source_notebook_id} in {source_cloud} workspace.")
        # Use default permissions if no permissions are found
        source_permissions = {
            "somin.sangwan@digivatelabs.com": "CAN_MANAGE",
            "samir.shinde@digivatelabs.com": "CAN_MANAGE"
        }

    # Fetch the notebook ID in the target workspace
    print(f"Fetching notebook ID in {target_cloud} workspace...")
    target_notebook_id = get_notebook_id(target_config["url"], target_config["token"], target_notebook_path)
    if not target_notebook_id:
        print(f"Failed to fetch notebook ID for {target_notebook_path} in {target_cloud} workspace.")
        return

    # Apply permissions to the source workspace
    print(f"Applying permissions to {source_cloud} workspace...")
    for user, permission_level in source_permissions.items():
        grant_permissions(source_config["url"], source_config["token"], source_notebook_path, user, permission_level, cluster_id)

    # Apply permissions to the target workspace
    print(f"Applying permissions to {target_cloud} workspace...")
    for user, permission_level in source_permissions.items():
        grant_permissions(target_config["url"], target_config["token"], target_notebook_path, user, permission_level, cluster_id)

    print("Notebook synchronization and permission sync completed.")


if __name__ == "__main__":
    source_cloud = input("Enter source cloud provider (AWS/AZURE/GCP): ")
    target_cloud = input("Enter target cloud provider (AWS/AZURE/GCP): ")
    git_url = input("Enter Git repository URL (optional): ")
    cluster_id = input("Enter cluster ID for attach permissions (optional): ")

    sync_notebooks_and_permissions(source_cloud, target_cloud, git_url, cluster_id)