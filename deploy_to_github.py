import os
import subprocess

# Simulate the environment for testing
os.environ['GITHUB_USERNAME'] = 'VandIProjects'
os.environ['GITHUB_TOKEN'] = 'test_token'

# GitHub credentials
github_username = os.getenv("GITHUB_USERNAME", "VandIProjects")
github_token = os.getenv("GITHUB_TOKEN")
if not github_token:
    raise ValueError("GitHub token is not set. Please set the GITHUB_TOKEN environment variable.")

repo_name = "youtube_scraper_package"
repo_url = f"https://{github_username}:{github_token}@github.com/{github_username}/{repo_name}.git"

# Local repository path (updated to match the screenshot)
local_repo_path = "/Blackliquid Productions (2)/Macintosh SSD/Users/iheanyingwaba/test_repo/youtube_scraper_package/youtubescraper_package"

try:
    # Navigate to the local repository path
    if not os.path.exists(local_repo_path):
        raise FileNotFoundError(f"Local repository path does not exist: {local_repo_path}")
    os.chdir(local_repo_path)

    # Initialize a Git repository
    subprocess.run(["git", "init"], check=True)

    # Add all files to the repository
    subprocess.run(["git", "add", "-A"], check=True)

    # Commit the changes
    subprocess.run(["git", "commit", "-m", "Initial commit: Add YouTube scraper workflow"], check=True)

    # Add the remote repository
    subprocess.run(["git", "remote", "add", "origin", repo_url], check=True)

    # Push the changes to GitHub
    subprocess.run(["git", "push", "-u", "origin", "main"], check=True)

    print(f"Workflow successfully deployed to GitHub repository: {repo_url}")

except FileNotFoundError as fnf_error:
    print(f"Error: {fnf_error}")
except subprocess.CalledProcessError as cpe_error:
    print(f"Git command failed: {cpe_error}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
