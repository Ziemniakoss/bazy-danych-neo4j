import datetime


class User:
    def __init__(self, asDict, isOriginal=False):
        if "type" in asDict:
            self.type = asDict["type"]
        else:
            self.type = "User"
        self.name = asDict["login"]
        if "blog" in asDict:
            self.blog = asDict["blog"]
        else:
            self.blog = None
        if "email" in asDict:
            self.email = asDict["email"]
        else:
            self.email= None
        self.isOriginal = isOriginal
        self.id=asDict["id"]

    def isOrg(self):
        return self.type != "User"


class GistFile:
    def __init__(self, asDict):
        self.name = asDict["filename"]
        self.type = asDict["type"]
        self.language = asDict["language"]
        if self.language is None:
            if self.type == "text/plain":
                self.language = "Text"
            else:
                self.language = "Unknown"
        self.size = asDict["size"]


class Gist:
    def __init__(self, asDict):
        self.id = asDict["id"]
        self.description = asDict["description"]
        filesMap = asDict["files"]
        self.files = [GistFile(filesMap[fileName]) for fileName in asDict["files"]]
        self.owner = User(asDict["owner"])


class Issue:
    def __init__(self, asDict, repoFullName: str):
        self.title = asDict["title"]
        self.id = asDict["id"]
        self.user = User(asDict["user"])
        self.body = asDict["body"]
        self.createdAt = asDict["created_at"]
        self.repoFullName = repoFullName


class PullRequest:
    def __init__(self, asDict, repoFullName: str):
        self.title = asDict["title"]
        self.id = asDict["id"]
        self.user = User(asDict["user"])
        self.body = asDict["body"]
        self.createdAt = asDict["created_at"]
        self.repoFullName = repoFullName


class License:
    def __init__(self, asDict):
        self.key = asDict["key"]
        self.name = asDict["name"]
        self.spdxId = asDict["spdx_id"]
        self.url = asDict["url"]


class Repo:
    def __init__(self, asDict):
        self.id = asDict["id"]
        self.name = asDict["name"]
        self.fullName = asDict["full_name"]
        self.owner = User(asDict["owner"])
        self.language=asDict["language"]
        self.homepage = asDict["homepage"]
        self.defaultBranch = asDict["default_branch"]
        self.description = asDict["description"]
        if "license" in asDict and asDict["license"] is not None:
            self.license = License(asDict["license"])
        else:
            self.license = None
        self.topics = asDict["topics"]


class RemainingLimit:
    def __init__(self, asDict):
        rate = asDict["resources"]["core"]
        self.used = rate["used"]
        self.limit = rate["limit"]
        self.remaining = rate["remaining"]
        self.reset = datetime.datetime.fromtimestamp(rate["reset"])
