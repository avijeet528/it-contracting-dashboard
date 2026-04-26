# extractor/github_pusher.py
# ═══════════════════════════════════════════════
# Pushes catalog_data.json to GitHub repo
# so GitHub Pages dashboard auto-updates
# ═══════════════════════════════════════════════

import base64
import json
import requests
from datetime import datetime
from config import GITHUB_TOKEN, GITHUB_REPO


class GitHubPusher:

    def __init__(self):
        self.token   = GITHUB_TOKEN
        self.repo    = GITHUB_REPO
        self.headers = {
            "Authorization": f"token {self.token}",
            "Content-Type":  "application/json",
            "Accept":        "application/vnd.github.v3+json",
        }
        self.base_url = "https://api.github.com"

    def _check_credentials(self):
        """Verify GitHub token and repo are set"""
        if not self.token:
            raise ValueError(
                "GITHUB_TOKEN not set in environment"
            )
        if not self.repo:
            raise ValueError(
                "GITHUB_REPO not set (format: username/repo)"
            )

    def _get_file_sha(self, filepath):
        """
        Get current SHA of a file in GitHub.
        Required for updating existing files.
        Returns SHA string or empty string if new file.
        """
        r = requests.get(
            f"{self.base_url}/repos/{self.repo}"
            f"/contents/{filepath}",
            headers=self.headers,
            timeout=30
        )
        if r.status_code == 200:
            return r.json().get("sha", "")
        return ""

    def push_file(self, local_filepath, repo_filepath=None):
        """
        Push a local file to GitHub repository.
        Creates or updates the file.
        """
        self._check_credentials()

        if repo_filepath is None:
            repo_filepath = local_filepath

        print(
            f"\n📤 Pushing to GitHub: "
            f"{self.repo}/{repo_filepath}"
        )

        # Read local file
        try:
            with open(local_filepath, "rb") as f:
                content = base64.b64encode(f.read()).decode()
        except FileNotFoundError:
            print(f"❌ File not found: {local_filepath}")
            return False

        # Get existing SHA for update
        sha = self._get_file_sha(repo_filepath)

        # Build commit payload
        payload = {
            "message": (
                f"Auto-update catalog data — "
                f"{datetime.now().strftime('%Y-%m-%d %H:%M')} UTC"
            ),
            "content": content,
            "branch":  "main",
        }
        if sha:
            payload["sha"] = sha
            action = "Updating"
        else:
            action = "Creating"

        print(f"   {action} {repo_filepath}...")

        # Push to GitHub
        r = requests.put(
            f"{self.base_url}/repos/{self.repo}"
            f"/contents/{repo_filepath}",
            headers=self.headers,
            json=payload,
            timeout=60
        )

        if r.status_code in [200, 201]:
            commit_sha = r.json()["commit"]["sha"][:8]
            print(
                f"   ✅ Pushed successfully "
                f"(commit: {commit_sha})"
            )
            print(
                f"   🌐 Dashboard: "
                f"https://{self.repo.split('/')[0]}.github.io"
                f"/{self.repo.split('/')[1]}/"
            )
            return True
        else:
            print(
                f"   ❌ Push failed: "
                f"{r.status_code} — {r.text[:200]}"
            )
            return False

    def push_catalog(self, local_path="catalog_data.json"):
        """Push catalog_data.json to GitHub root"""
        return self.push_file(local_path, "catalog_data.json")

    def push_multiple(self, files_dict):
        """
        Push multiple files.
        files_dict: { local_path: repo_path, ... }
        """
        results = {}
        for local, remote in files_dict.items():
            results[local] = self.push_file(local, remote)
        return results

    def trigger_pages_rebuild(self):
        """Trigger GitHub Pages rebuild"""
        r = requests.post(
            f"{self.base_url}/repos/{self.repo}/pages/builds",
            headers=self.headers,
            timeout=30
        )
        if r.status_code in [200, 201]:
            print("   ✅ GitHub Pages rebuild triggered")
        else:
            print(f"   ⚠️  Pages rebuild: {r.status_code}")

    def get_latest_commit(self):
        """Get info about latest commit"""
        r = requests.get(
            f"{self.base_url}/repos/{self.repo}"
            f"/commits/main",
            headers=self.headers,
            timeout=30
        )
        if r.status_code == 200:
            data = r.json()
            return {
                "sha":     data["sha"][:8],
                "message": data["commit"]["message"],
                "date":    data["commit"]["author"]["date"],
                "author":  data["commit"]["author"]["name"],
            }
        return {}

    def test_connection(self):
        """Test GitHub connection"""
        r = requests.get(
            f"{self.base_url}/repos/{self.repo}",
            headers=self.headers,
            timeout=30
        )
        if r.status_code == 200:
            data = r.json()
            print(f"✅ GitHub connected: {self.repo}")
            print(
                f"   Visibility: "
                f"{'Private' if data.get('private') else 'Public'}"
            )
            print(
                f"   Default branch: "
                f"{data.get('default_branch', 'main')}"
            )
            return True
        else:
            print(
                f"❌ GitHub connection failed: "
                f"{r.status_code}"
            )
            return False
