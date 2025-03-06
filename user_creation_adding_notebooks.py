import os
import requests
import base64
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration for source and destination workspaces
WORKSPACE_CONFIG = {
    "STAGING": {
        "url": os.getenv("STAGING_WORKSPACE_URL"),
        "token": os.getenv("STAGING_ACCESS_TOKEN"),
    },
    "PREPROD": {
        "url": os.getenv("PREPROD_WORKSPACE_URL"),
        "token": os.getenv("PREPROD_ACCESS_TOKEN"),
    },
}

def get_headers(token):
    """Return headers required for API requests."""
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

def get_workspace_config(workspace_name):
    """Retrieve workspace configuration."""
    config = WORKSPACE_CONFIG.get(workspace_name.upper(), None)
    if not config:
        print(f"Workspace configuration not found for: {workspace_name}")
    return config

def get_users(workspace_url, access_token):
    """Fetch all users from a Databricks workspace."""
    api_endpoint = f"{workspace_url}/api/2.0/preview/scim/v2/Users"
    try:
        response = requests.get(api_endpoint, headers=get_headers(access_token))
        response.raise_for_status()
        users = response.json().get("Resources", [])
        print(f"Fetched {len(users)} users from workspace.")
        return users
    except requests.exceptions.RequestException as e:
        print(f"Error fetching users: {e}")
        return []

def create_user(workspace_url, access_token, user):
    """Create a user in a Databricks workspace."""
    api_endpoint = f"{workspace_url}/api/2.0/preview/scim/v2/Users"
    try:
        response = requests.post(api_endpoint, headers=get_headers(access_token), json=user)
        response.raise_for_status()
        print(f"Created user: {user['userName']}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error creating user {user['userName']}: {e}")
        return False

def list_notebooks(workspace_url, access_token, path="/"):
    """Recursively list all notebooks and directories in a Databricks workspace."""
    api_endpoint = f"{workspace_url}/api/2.0/workspace/list"
    params = {"path": path}
    notebooks = []
    directories = set()
    try:
        response = requests.get(api_endpoint, headers=get_headers(access_token), params=params)
        response.raise_for_status()
        objects = response.json().get("objects", [])

        for obj in objects:
            if obj["object_type"] == "NOTEBOOK":
                notebooks.append(obj["path"])
            elif obj["object_type"] == "DIRECTORY":
                directories.add(obj["path"])
                sub_notebooks, sub_dirs = list_notebooks(workspace_url, access_token, obj["path"])
                notebooks.extend(sub_notebooks)
                directories.update(sub_dirs)

        return notebooks, directories
    except requests.exceptions.RequestException as e:
        print(f"Error listing notebooks in {path}: {e}")
        return [], set()

def export_notebook(workspace_url, access_token, notebook_path):
    """Export a notebook from a Databricks workspace and decode the base64 content."""
    api_endpoint = f"{workspace_url}/api/2.0/workspace/export"
    params = {"path": notebook_path, "format": "SOURCE"}

    try:
        response = requests.get(api_endpoint, headers=get_headers(access_token), params=params)
        response.raise_for_status()
        content = response.json().get("content", "")

        # Decode the base64-encoded content
        return base64.b64decode(content).decode("utf-8")
    except requests.exceptions.RequestException as e:
        print(f"Error exporting notebook {notebook_path}: {e}")
        return None

def create_directory(workspace_url, access_token, directory_path):
    """Create a directory in the Databricks workspace."""
    api_endpoint = f"{workspace_url}/api/2.0/workspace/mkdirs"
    data = {"path": directory_path}

    try:
        response = requests.post(api_endpoint, headers=get_headers(access_token), json=data)
        response.raise_for_status()
        print(f"Created directory: {directory_path}")
    except requests.exceptions.RequestException as e:
        print(f"Error creating directory {directory_path}: {e}")

def import_notebook(workspace_url, access_token, notebook_path, content):
    """Import a notebook into a Databricks workspace."""
    api_endpoint = f"{workspace_url}/api/2.0/workspace/import"

    # Encode the notebook content to base64
    content_encoded = base64.b64encode(content.encode('utf-8')).decode('utf-8')

    data = {
        "path": notebook_path,
        "format": "SOURCE",
        "content": content_encoded,
        "overwrite": True,
        "language": "PYTHON"  # Or the appropriate language
    }

    try:
        response = requests.post(api_endpoint, headers=get_headers(access_token), json=data)
        response.raise_for_status()
        print(f"Imported notebook: {notebook_path}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error importing notebook {notebook_path}: {e}")
        return False

def transfer_users_and_notebooks(source_workspace, target_workspace):
    """Transfer users and notebooks while keeping the original structure."""
    print("Starting user and notebook transfer...")

    source_config = get_workspace_config(source_workspace)
    target_config = get_workspace_config(target_workspace)

    if not source_config:
        print(f"Invalid source workspace: {source_workspace}")
        return
    if not target_config:
        print(f"Invalid target workspace: {target_workspace}")
        return

    # Transfer users
    print(f"Fetching users from {source_workspace}...")
    users = get_users(source_config["url"], source_config["token"])

    print(f"Creating users in {target_workspace}...")
    user_name_map = {}
    for user in users:
        user.pop("id", None)  # Remove the 'id' field
        if create_user(target_config["url"], target_config["token"], user):
            user_name_map[user['userName']] = user.get('userName', 'unknown_user')

    # Transfer notebooks and directories
    print(f"Listing all notebooks and directories in {source_workspace}...")
    notebook_paths, directory_paths = list_notebooks(source_config["url"], source_config["token"])

    print(f"Found {len(directory_paths)} directories and {len(notebook_paths)} notebooks.")

    # Create directories first to maintain structure
    for directory in sorted(directory_paths):
        print(f"Creating directory in {target_workspace}: {directory}")
        create_directory(target_config["url"], target_config["token"], directory)

    # Transfer notebooks
    for notebook_path in notebook_paths:
        print(f"Processing notebook: {notebook_path}")

        notebook_content = export_notebook(source_config["url"], source_config["token"], notebook_path)
        if not notebook_content:
            print(f"Skipping notebook {notebook_path} due to export error.")
            continue

        # Import to the same path in the target workspace
        print(f"Importing notebook to {target_workspace}: {notebook_path}")
        if not import_notebook(target_config["url"], target_config["token"], notebook_path, notebook_content):
            print(f"Failed to import notebook: {notebook_path}")

    print("User and notebook transfer completed.")

if __name__ == "__main__":
    source_workspace = input("Enter source workspace (STAGING/PREPROD): ")
    target_workspace = input("Enter target workspace (STAGING/PREPROD): ")

    transfer_users_and_notebooks(source_workspace, target_workspace)
