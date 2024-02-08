"""
This module is designed to interact with GitHub's API to fetch data about repositories and pull requests.
It utilizes environment variables for configuration and handles pagination and rate limiting.
"""

import json
import os
from pathlib import Path
import time

import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
GITHUB_API_TOKEN = os.getenv("GITHUB_API_TOKEN")
GITHUB_ORGANIZATION = os.getenv("GITHUB_ORGANIZATION")


def fetch_data(url):
    """
    Fetch data from a given URL with pagination and rate limiting handling.

    :param url: URL to fetch data from
    :return: A list of all fetched data items
    """
    headers = {
        "Authorization": f"token {GITHUB_API_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    all_data = []
    while url:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            all_data.extend(data)

            # Pagination: Check for 'Link' header
            link_header = response.headers.get("Link", None)
            if link_header:
                links = link_header.split(", ")
                next_link = [link for link in links if 'rel="next"' in link]
                if next_link:
                    url = next_link[0].split(";")[0].strip("<>").strip()
                else:
                    url = None
            else:
                url = None
        elif (
            response.status_code == 403
            and "X-RateLimit-Remaining" in response.headers
            and int(response.headers["X-RateLimit-Remaining"]) == 0
        ):
            # Rate limit exceeded: Wait until the rate limit resets
            reset_time = int(response.headers["X-RateLimit-Reset"])
            sleep_time = (
                reset_time - int(time.time()) + 1
            )  # Add a second to ensure the limit has been reset
            print(f"Rate limit exceeded. Waiting {sleep_time} seconds to retry...")
            time.sleep(sleep_time)
            continue  # Retry the request
        else:
            print(
                f"Failed to fetch data from {url} with status code {response.status_code}"
            )
            break
    return all_data


def save_df_as_json(data, path, filename):
    """
    Save data as JSON to a specified path.

    :param data: Data to save as JSON
    :param path: Directory path to save the JSON file
    :param filename: Name of the file to save the data in
    """
    if data:
        os.makedirs(path, exist_ok=True)
        full_path = os.path.join(path, filename)
        with open(full_path, "w", encoding="utf-8") as json_file:  # Specify encoding
            json.dump(data, json_file)
        print(f"Data saved to {full_path}")
    else:
        print("No data to save.")


def fetch_repositories(organization):
    """
    Fetch repositories and return the raw data as JSON.

    :param organization: GitHub organization name
    :return: JSON data of repositories
    """
    repos_url = f"https://api.github.com/orgs/{organization}/repos"
    return fetch_data(repos_url)


def fetch_pull_requests(organization, repositories):
    """
    Fetch pull requests for each repository and return the raw data as JSON.

    :param organization: GitHub organization name
    :param repositories: List of repository names
    :return: JSON data of pull requests
    """
    pr_data = []
    for repo in repositories:
        prs_url = f"https://api.github.com/repos/{organization}/{repo}/pulls?state=all"
        prs = fetch_data(prs_url)
        if prs:
            for pr in prs:
                pr["repository"] = repo
            pr_data.extend(prs)
    return pr_data


def main():
    """
    Main function to orchestrate fetching and saving GitHub data.
    """
    organization = GITHUB_ORGANIZATION

    # Define the project root path and construct the dynamic path using pathlib
    project_root_path = (
        Path(__file__).resolve().parents[2]
    )  # Adjust the parent count as needed
    dynamic_path = project_root_path / "data" / "input"

    # Ensure the dynamic path directory exists
    dynamic_path.mkdir(parents=True, exist_ok=True)

    # Step 1: Fetch repository data and save it
    repos_data = fetch_repositories(organization)
    save_df_as_json(
        repos_data,
        dynamic_path,
        f'{organization.lower().replace("-", "_")}_repositories.json',
    )

    # Step 2: Fetch PR data for these repositories and save it
    if repos_data:
        repositories = [repo["name"] for repo in repos_data]
        pr_data = fetch_pull_requests(organization, repositories)
        save_df_as_json(
            pr_data,
            dynamic_path,
            f'{organization.lower().replace("-", "_")}_pull_requests.json',
        )


if __name__ == "__main__":
    main()
