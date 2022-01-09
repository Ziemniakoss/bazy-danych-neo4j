import datetime
import json
import os.path
import sys
from time import sleep
from typing import List
import requests as req

from data_types import User, Repo, Issue, PullRequest, Gist, RemainingLimit

FILE_WITH_USERS_TO_FETCH = "users_to_fetch.txt"
GITHUB_DOMAIN = "https://api.github.com"
GITHUB_CACHE_FOLDER = ".cached_results"


def wait(end):
    while True:
        now = datetime.datetime.now()
        if now > end:
            return
        print("[SLEEP]", f"waiting to {end}")
        sleep(60)


class Github:
    def __init__(self):
        self.limits = None
        self.limits = self.fetchRemainingRequests()
        print(self.limits.remaining)

    def getLimits(self):
        return self.limits

    def fetchUser(self, userName: str) -> User:
        url = f"{GITHUB_DOMAIN}/users/{userName}"
        return User(self.fetchFrom(url))

    def fetchRepositories(self, user: User) -> List[Repo]:
        url = f"{GITHUB_DOMAIN}/users/{user.name}/repos"
        print(f"[{user.name}/repos]", "fetching for", user.name)
        repos = [Repo(repo) for repo in self.fetchFrom(url)]
        return repos

    def fetchIssues(self, repo: Repo) -> List[Issue]:
        url = f"{GITHUB_DOMAIN}/repos/{repo.fullName}/issues"
        print(f"[{repo.fullName}/issues]", "fetching issues for", repo.fullName)
        issues = [Issue(issue, repo.fullName) for issue in self.fetchFrom(url)]
        return issues

    def fetchPullRequests(self, repo: Repo) -> List[PullRequest]:
        url = f"{GITHUB_DOMAIN}/repos/{repo.fullName}/pulls"
        print(f'[{repo.fullName}/pulls]', "fetching pull requests for", repo.fullName)
        pullRequest = [PullRequest(pr, repo.fullName) for pr in self.fetchFrom(url)]
        return pullRequest

    def fetchGists(self, user: User) -> List[Gist]:
        url = f"{GITHUB_DOMAIN}/users/{user.name}/gists"
        print(f"[{user.name}/gists]", "fetching gists for", user.name)
        gists = [Gist(g) for g in self.fetchFrom(url)]
        return gists

    def fetchContibutors(self, repo: Repo) -> List[User]:
        url = f"{GITHUB_DOMAIN}/repos/{repo.fullName}/contributors"
        print(f"[{repo.fullName}/contributions]", "fetching for", repo.fullName)
        contrs = [User(u) for u in self.fetchFrom(url)]
        return contrs

    def fetchSubscriptions(self, repo: Repo):
        url = f"{GITHUB_DOMAIN}/repos/{repo.fullName}/subscribers"
        print(f"[{repo.fullName}/subs]", "fetching for", repo.fullName)
        subscriptions = [User(u) for u in self.fetchFrom(url)]
        return subscriptions

    def fetchRemainingRequests(self) -> RemainingLimit:
        url = f"{GITHUB_DOMAIN}/rate_limit"
        return RemainingLimit(self.fetchFrom(url, True))

    def saveToCache(self, url: str, data):
        if not os.path.exists(GITHUB_CACHE_FOLDER):
            os.mkdir(GITHUB_CACHE_FOLDER)
        with open(self.getPathToCached(url), "w") as out:
            out.write(data)

    def getPathToCached(self, url: str) -> str:
        fileName = url.replace(f"{GITHUB_DOMAIN}/", "").replace("/", "__") + ".json"
        return os.path.join(GITHUB_CACHE_FOLDER, fileName)

    def fetchFrom(self, url: str, skipCache=False, skipLimitsCheck=False):
        cachedFilePath = self.getPathToCached(url)
        if os.path.exists(cachedFilePath) and not skipCache:
            with open(cachedFilePath, "r") as cachedVersionFile:
                return json.load(cachedVersionFile)

        if self.limits is not None and self.limits.remaining == 0:
            wait(self.limits.reset)
            self.limits = None
            self.limits = self.fetchRemainingRequests()
        response = req.get(url)
        if not response.ok:
            raise Exception(response.text)
        if self.limits is not None:
            self.limits.remaining -= 1
            print("[LIMITS]", "left", self.limits.remaining, "from", self.limits.limit, "until", self.limits.reset)
        respText = response.text
        self.saveToCache(url, respText)
        return json.loads(respText)


FETCHED_USERS_FILE = ".fetched_users.txt"


def fetchUserData(userName: str, github: Github):
    user = None
    try:
        user = github.fetchUser(userName)
    except Exception as e:
        logError(f"__user__{userName}", e)
        return

    try:
        github.fetchGists(user)
    except Exception as e:
        logError(f"{user.name}/gists", e)
    repos = []
    try:
        repos = github.fetchRepositories(user)
    except Exception as e:
        logError(f"{user.name}/repos", e)

    reposCount = len(repos)
    for i in range(reposCount):
        print("[REPOS]", f"{i}/{reposCount}")
        repo = repos[i]
        try:
            github.fetchIssues(repo)
        except (Exception) as e:
            logError(f"{repo.fullName}/issues", e)
        try:
            github.fetchPullRequests(repo)
        except Exception as e:
            logError(f"{repo.fullName}/pullrequests", e)
        try:
            github.fetchContibutors(repo)
        except Exception as e:
            logError(f"{repo.fullName}/contributuons", e)
        try:
            github.fetchSubscriptions(repo)
        except Exception as e:
            logError(f"{repo.fullName}/subscriptions", e)
    pass


def logError(title, message):
    message = str(message)
    if not os.path.exists(".errors"):
        os.mkdir(".errors")
    errorPath = os.path.join(".errors", title.replace("/", "__"))
    print("[ERROR]", f"[{title}]", message, file=sys.stderr)
    with open(errorPath, "w") as file:
        file.write(message)
        file.write("\n")


if __name__ == '__main__':
    github = Github()
    with open(FILE_WITH_USERS_TO_FETCH) as usersList:
        for username in usersList:
            username = username.strip()
            print("[USER]", "fetching", username, "data")
            fetchUserData(username, github)
            with open(FETCHED_USERS_FILE, "a") as fetchedUsersFile:
                fetchedUsersFile.write(username)
                fetchedUsersFile.write("\n")
