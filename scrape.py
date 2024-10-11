import os
import requests
import pandas as pd
import time

# GitHub token for authentication
TOKEN = os.environ["GITHUB_API_TOKEN"]
HEADERS = {"Authorization": f"token {TOKEN}"}

# Cache for storing GitHub usernames and their associated display names
user_display_name_cache = {}


# Function to handle GitHub rate limiting
def check_rate_limit(response):
    remaining = int(response.headers.get("X-RateLimit-Remaining", 1))
    if remaining == 0:
        reset_time = int(response.headers.get("X-RateLimit-Reset"))
        sleep_time = reset_time - int(time.time()) + 1
        print(f"Rate limit exceeded. Sleeping for {sleep_time} seconds...")
        time.sleep(sleep_time)
    return remaining


# Function to handle pagination for any GitHub API endpoint
def fetch_paginated_data(url):
    page = 1
    results = []

    while True:
        print(page)
        paginated_url = f"{url}?page={page}&per_page=100"
        retries = 3
        response = None
        while retries > 0:
            response = requests.get(paginated_url, headers=HEADERS)
            check_rate_limit(response)
            if response.status_code != 200:
                print(
                    f"Failed to fetch data: {response.status_code}"
                    f", {paginated_url}"
                )
                retries -= 1
                time.sleep(1)
                continue
            else:
                break

        if response is None or response.status_code != 200:
            # We tried, so stop paginating
            break

        data = response.json()
        if not data:
            break  # No more data, end of pagination
        results.extend(data)
        page += 1

    return results


# Function to fetch a user's display name from GitHub API
def fetch_display_name(username):
    # Check if the username is already cached
    if username in user_display_name_cache:
        return user_display_name_cache[username]

    print(f"Fetching display name for {username}")

    user_url = f"https://api.github.com/users/{username}"
    response = requests.get(user_url, headers=HEADERS)
    check_rate_limit(response)

    if response.status_code == 200:
        user_data = response.json()
        display_name = user_data.get(
            "name", ""
        )  # Fetch 'name' from the user's profile
        user_display_name_cache[username] = display_name  # Cache the result
        return display_name
    else:
        print(f"Failed to fetch user profile for {username}")
        user_display_name_cache[username] = (
            ""  # Cache empty result for failed request
        )
        return ""


# Function to fetch commits from a repository
def fetch_commits(repo):
    commits_url = f"https://api.github.com/repos/{repo}/commits"
    data = fetch_paginated_data(commits_url)

    commits = []
    for commit in data:
        username = commit["author"]["login"] if commit["author"] else "Unknown"
        display_name = fetch_display_name(username)

        # Fallback to commit author's name if no GitHub display name is set
        if not display_name:
            display_name = commit["commit"]["author"]["name"]

        commits.append(
            {
                "sha": commit["sha"],  # Added the SHA field
                "timestamp": commit["commit"]["author"]["date"],
                "username": username,
                "name": display_name,
                "action_type": "commit",
                "repository": repo,
            }
        )
    return commits


# Function to fetch issue comments
def fetch_issue_comments(repo):
    issue_comments_url = f"https://api.github.com/repos/{repo}/issues/comments"
    data = fetch_paginated_data(issue_comments_url)

    comments = []
    for comment in data:
        username = comment["user"]["login"] if comment["user"] else "Unknown"
        display_name = (
            fetch_display_name(username)
            if username != "Unknown"
            else "Unknown"
        )

        comments.append(
            {
                "timestamp": comment["created_at"],
                "username": username,
                "name": display_name,  # Consistent display name fetched via API
                "action_type": "issue comment",
                "repository": repo,
            }
        )
    return comments


# Function to fetch pull request reviews
def fetch_pull_reviews(repo):
    reviews_url = f"https://api.github.com/repos/{repo}/pulls/comments"
    data = fetch_paginated_data(reviews_url)

    reviews = []
    for review in data:
        username = review["user"]["login"] if review["user"] else "Unknown"
        display_name = (
            fetch_display_name(username)
            if username != "Unknown"
            else "Unknown"
        )

        reviews.append(
            {
                "timestamp": review["created_at"],
                "username": username,
                "name": display_name,  # Consistent display name fetched via API
                "action_type": "PR comment",
                "repository": repo,
            }
        )
    return reviews


# Function to filter out duplicate commits between repositories
def filter_duplicate_commits(source_commits, target_commits):
    source_shas = {commit["sha"] for commit in source_commits}
    filtered_commits = [
        commit for commit in target_commits if commit["sha"] not in source_shas
    ]
    return filtered_commits


# Function to fetch opened pull requests
def fetch_pull_requests(repo):
    pulls_url = f"https://api.github.com/repos/{repo}/pulls"
    pulls = fetch_paginated_data(pulls_url)

    pr_events = []
    for pr in pulls:
        username = pr["user"]["login"] if pr["user"] else "Unknown"
        display_name = (
            fetch_display_name(username)
            if username != "Unknown"
            else "Unknown"
        )

        pr_events.append(
            {
                "timestamp": pr["created_at"],
                "username": username,
                "name": display_name,
                "action_type": "PR opened",
                "repository": repo,
            }
        )
    return pr_events


# Function to fetch opened issues
def fetch_issues(repo):
    issues_url = f"https://api.github.com/repos/{repo}/issues"
    issues = fetch_paginated_data(issues_url)

    issue_events = []
    for issue in issues:
        # Skip pull requests since they are also listed in the issues API
        if "pull_request" not in issue:
            username = issue["user"]["login"] if issue["user"] else "Unknown"
            display_name = (
                fetch_display_name(username)
                if username != "Unknown"
                else "Unknown"
            )

            issue_events.append(
                {
                    "timestamp": issue["created_at"],
                    "username": username,
                    "name": display_name,
                    "action_type": "issue opened",
                    "repository": repo,
                }
            )
    return issue_events


# Modified main function to collect all data
def collect_data(repos, arrow_commits):
    all_data = []

    for repo in repos:
        print(f"Fetching data for repository: {repo}")
        if repo != "apache/arrow":
            commits = fetch_commits(repo)
            # Filter out commits from apache/arrow
            commits_filtered = filter_duplicate_commits(arrow_commits, commits)
            all_data.extend(commits_filtered)
        else:
            all_data.extend(arrow_commits)

        # Fetch pull request opened, issue opened, issue comments, and pull reviews
        all_data.extend(fetch_pull_requests(repo))
        all_data.extend(fetch_issues(repo))
        all_data.extend(fetch_issue_comments(repo))
        all_data.extend(fetch_pull_reviews(repo))

    return pd.DataFrame(all_data)


# Fetch commits from apache/arrow first
print("Fetching data for repository: apache/arrow")
arrow_commits = fetch_commits("apache/arrow")

repos = [
    "apache/arrow-adbc",
    "apache/arrow-cookbook",
    "apache/arrow-go",
    "apache/arrow-julia",
    "apache/arrow-nanoarrow",
    "apache/arrow-rs",
    "apache/arrow-site",
    "apache/arrow-testing",
    "apache/arrow",
]  # Add your repositories here

df = collect_data(repos, arrow_commits)

del df["sha"]
df["timestamp"] = pd.to_datetime(df["timestamp"])
df = df.sort_values(by="timestamp").reset_index(drop=True)
df.to_csv("activity.csv", index=False)
