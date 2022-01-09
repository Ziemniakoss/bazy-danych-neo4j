import json
import os.path
import sys
from typing import Dict

from neo4j import GraphDatabase

from data_types import User, Issue, PullRequest, Gist, GistFile, Repo, License

DB_URI = "neo4j://localhost:7687"


def getOrDefault(d: Dict, keyName: str, defaultValue):
    if keyName in d:
        return d[keyName]
    return defaultValue


class Neo4JConnection:
    def __init__(self):
        self.driver = GraphDatabase.driver(DB_URI)

    def close(self):
        self.driver.close()
        self.driver = None

    def createUserOrOrg(self, user: User):
        with self.driver.session() as ses:
            ses.write_transaction(self.createAndReturnUser, user)

    @staticmethod
    def createAndReturnUser(tx, user: User):
        query = f"""CREATE (a:{user.type}) 
        SET a.blog = $blog
        SET a.id = $id
        SET a.email = $email
        SET a.name = $login
        RETURN a
        """
        result = tx.run(query,
                        id=user.id,
                        blog=user.blog,
                        login=user.name,
                        email=user.email,
                        )
        return result.single()[0]

    def createGist(self, user: User, gist: Gist):
        with self.driver.session() as session:
            session.write_transaction(self.__createGist, user, gist)
        pass

    @staticmethod
    def __createGist(tx, user: User, gist: Gist):
        query = """
        MERGE (owner {id: $user_id, name: $user_name}) 
        CREATE (g:Gist) 
        SET g.id = $gist_id
        SET g.description = $gist_description
        
        CREATE (owner)-[:CREATED]->(g)
        """
        query += f" SET owner :{user.type} "
        tx.run(query,
               user_name=user.name,
               user_id=user.id,
               user_type=user.type,
               gist_id=gist.id,
               gist_description=gist.description,
               )

    def createGistFile(self, gist, file: GistFile):
        with self.driver.session() as ses:
            ses.write_transaction(self.__createGistFile, gist, file)

    @staticmethod
    def __createGistFile(tx, gist: Gist, file: GistFile):
        query = """
        MATCH (gi:Gist {id: $gist_id})
        MERGE (lang:Language {name: $lang})
        CREATE (gf:GistFile {
            name: $gf_name,
            type: $gf_type,
            size: $gf_size        
        }),
        (gi)-[:CONTAINS]->(gf),
        (gf)-[:IS_WRITTEN_IN]->(lang)
        """
        tx.run(query,
               gist_id=gist.id,
               gf_name=file.name,
               lang=file.language,
               gf_type=file.type,
               gf_size=file.size)

    def createRepo(self, repo: Repo):
        with self.driver.session() as s:
            s.write_transaction(self.__createRepo, repo)

    @staticmethod
    def __createRepo(tx, repo: Repo):
        query = """
        MERGE (owner {id: $owner_id, name: $owner_name})
        CREATE (repo:Repository {
            id: $repo_id,
            name: $repo_name,
            fullName: $repo_fullName,
            description: $repo_desc,
            homepage: $repo_homepage,
            defaultBranch: $repo_branch
        }),
        (owner)-[:OWNS]->(repo)
        """
        if repo.language is not None:
            query += """
            
            MERGE (lang:Language {name: $lang})
            CREATE (repo)-[:IS_WRITTEN_IN]->(lang)          
            
            """
        query += f" SET owner :{repo.owner.type} "
        licenseKey = None
        licenseName = None
        licenseUrl = None
        licenseSpdxId = None
        if repo.license is not None:
            query += """
            
            MERGE (l:License{
                spdxId: $l_spdxId
            })
            SET l.key = $l_key
            SET l.name = $l_name
            SET l.url = $l_url
            """
            licenseKey = repo.license.key
            licenseName = repo.license.name
            licenseUrl = repo.license.url
            licenseSpdxId = repo.license.spdxId
        tx.run(query,
               owner_id=repo.owner.id,
               owner_name=repo.owner.name,
               owner_type=repo.owner.type,
               repo_id=repo.id,
               repo_name=repo.name,
               repo_fullName=repo.fullName,
               repo_desc=repo.description,
               repo_homepage=repo.homepage,
               repo_branch=repo.defaultBranch,
               lang=repo.language,
               l_key=licenseKey,
               l_name=licenseName,
               l_url=licenseUrl,
               l_spdxId=licenseSpdxId
               )
        Neo4JConnection.__createRepoTopics(tx, repo)

    @staticmethod
    def __createRepoTopics(tx, repo: Repo):
        for topic in repo.topics:
            query = """
            MATCH (r:Repository) WHERE r.fullName = $r_fullName
            MERGE (t:Topic {name: $t_name})
            CREATE (r)-[:RELATES_TO]->(t)
            """
            tx.run(query, t_name=topic, r_fullName=repo.fullName)

    def createContributorLink(self, repo: Repo, contributor: User):
        with self.driver.session() as s:
            s.write_transaction(self.__createContributorLink, repo, contributor)
        pass

    @staticmethod
    def __createContributorLink(tx, repo: Repo, contributor: User):
        Neo4JConnection.__createRelationBetweenUserAndRepo(tx, repo, contributor, "CONTRIBUTES")

    def createSubscriberLink(self, repo: Repo, user: User):
        with self.driver.session() as s:
            s.write_transaction(self.__createSubscriberLink, repo, user)

    @staticmethod
    def __createSubscriberLink(tx, repo: Repo, subscriber: User):
        Neo4JConnection.__createRelationBetweenUserAndRepo(tx, repo, subscriber, "SUBSCRIBES")

    @staticmethod
    def __createRelationBetweenUserAndRepo(tx, repo: Repo, user: User, relationName: str):
        query = f"""
               MATCH (repo:Repository) WHERE repo.fullName = $repo_fullName
               MERGE (user {{ id: $user_id, name: $user_name}})
               SET user :{user.type}
               CREATE (user)-[:{relationName}]->(repo)
               """
        tx.run(query,
               repo_fullName=repo.fullName,
               user_id=user.id,
               user_name=user.name)

    def createIssue(self, repo: Repo, issue: Issue):
        with self.driver.session() as s:
            s.write_transaction(self.__createIssue, repo, issue)

    @staticmethod
    def __createIssue(tx, repo: Repo, issue: Issue):
        query = f"""
        MATCH (repo:Repository) WHERE repo.id = $repo_id
        MERGE (creator {{id: $c_id, name: $c_name}})
        SET creator :{issue.user.type}
        CREATE (issue:Issue {{
            name: $is_name,
            body: $is_body,
            id: $is_id
        }}),
        (repo)-[:HAS]->(issue),
        (creator)-[:CREATED]->(issue)
        """
        tx.run(query,
               repo_id=repo.id,
               c_id=issue.user.id,
               c_name=issue.user.name,
               is_name=issue.title,
               is_body=issue.body,
               is_id=issue.id)

    def createPullRequest(self, repo: Repo, pullRequest: PullRequest):
        with self.driver.session() as s:
            s.write_transaction(self.__createPullRequest, repo, pullRequest)

    @staticmethod
    def __createPullRequest(tx, repo: Repo, pullRequest: PullRequest):
        query = f"""
        MATCH (repo:Repository) WHERE repo.id = $repo_id
        MERGE (creator {{id: $c_id, name: $c_name}})
        SET creator :{pullRequest.user.type}
        CREATE (pr:PullRequest {{
            name: $pr_name,
            body: $pr_body,
            id: $pr_id
        }}),
        (repo)-[:HAS]->(pr),
        (creator)-[:CREATED]->(pr)
        """
        tx.run(query,
               repo_id=repo.id,
               c_id=pullRequest.user.id,
               c_name=pullRequest.user.name,
               pr_name=pullRequest.title,
               pr_body=pullRequest.body,
               pr_id=pullRequest.id)


def importRepositories(conn: Neo4JConnection, user: User):
    filePath = os.path.join(".cached_results", f"users__{user.name}__repos.json")
    if not os.path.exists(filePath):
        print("[REPO]", f"[{user.name}]", "User has no repos file, skipping", file=sys.stderr)
        return
    with open(filePath) as f:
        print("[REPO]", f"[{user.name}] reading from disc")
        repos = [Repo(r) for r in json.loads(f.read())]
        for repo in repos:
            print("[REPO]", f"[{repo.fullName}]", "importing to neo4j")
            conn.createRepo(repo)
            importSubscribers(conn, repo)
            importPullRequests(conn, repo)
            importIssues(conn, repo)
            try:
                importContributors(conn, repo)
            except (Exception) as e:
                print("[CONTR]", f"[{repo.fullName}]", "failed to load contributors", file=sys.stderr)
                print(str(e), file=sys.stderr)

    pass


def importGists(conn: Neo4JConnection, user: User):
    pathToFileWIthGists = os.path.join(".cached_results", f"users__{user.name}__gists.json")
    if not os.path.exists(pathToFileWIthGists):
        print("[GIST]", f"[{user.name}]", "gists dont exist, skipping")
        return
    with open(pathToFileWIthGists) as gistsFile:
        parsedGists = json.loads(gistsFile.read())
        gists = [Gist(g) for g in parsedGists]
        for gist in gists:
            print("[GIST]", f"[{user.name}] importing {gist.id}")
            conn.createGist(user, gist)
            for file in gist.files:
                conn.createGistFile(gist, file)


def importContributors(conn: Neo4JConnection, repo: Repo):
    safeName = repo.fullName.replace("/", "__")
    fileWithContributions = os.path.join(".cached_results", f"repos__{safeName}__contributors.json")
    if not os.path.exists(fileWithContributions):
        print("[CONTR]", f"[{repo.fullName}]", "no contributors, skipping")
        return
    with open(fileWithContributions) as f:
        fileContent = f.read().strip()
        if fileContent == "":
            return
        contrsAsJson = json.loads(fileContent)
        contrs = [User(c) for c in contrsAsJson]
        for contr in contrs:
            print("[CONTR]", f"[{repo.fullName}]", f"[{contr.name}]", "importing")
            conn.createContributorLink(repo, contr)


def importIssues(conn: Neo4JConnection, repo: Repo):
    safeName = repo.fullName.replace("/", "__")
    fileWithIssues = os.path.join(".cached_results", f"repos__{safeName}__issues.json")
    if not os.path.exists(fileWithIssues):
        print("[ISSUE]", f"[{repo.fullName}]", "file does not exist, skipping")
        return
    with open(fileWithIssues) as f:
        issues = [Issue(i, repo.fullName) for i in json.loads(f.read())]
        for issue in issues:
            print("[ISSUE]", f"[{repo.fullName}]", "importing issue", issue.id)
            conn.createIssue(repo, issue)


def importPullRequests(conn: Neo4JConnection, repo: Repo):
    safeName = repo.fullName.replace("/", "__")
    fileWithPulls = os.path.join(".cached_results", f"repos__{safeName}__pulls.json")
    if not os.path.exists(fileWithPulls):
        print("[PULL]", f"[{repo.fullName}]", "file does not exist, skipping")
        return
    with open(fileWithPulls) as f:
        pullRequests = [PullRequest(p, repo.fullName) for p in json.loads(f.read())]
        for pullRequest in pullRequests:
            print("[PULL]", f"[{repo.fullName}]", "importing pull request", pullRequest.id)
            conn.createPullRequest(repo, pullRequest)
    pass


def importSubscribers(conn: Neo4JConnection, repo: Repo):
    safeName = repo.fullName.replace("/", "__")
    fileWithContributions = os.path.join(".cached_results", f"repos__{safeName}__subscribers.json")
    if not os.path.exists(fileWithContributions):
        print("[SUBSC]", f"[{repo.fullName}]", "no subcribers, skipping")
        return
    with open(fileWithContributions) as f:
        fileContent = f.read().strip()
        if fileContent == "":
            return
        subscriberAsJson = json.loads(fileContent)
        subscriber = [User(c) for c in subscriberAsJson]
        for sub in subscriber:
            print("[SUBSC]", f"[{repo.fullName}]", f"[{sub.name}]", "importing")
            conn.createSubscriberLink(repo, sub)


def importUserWithData(userName: str, conn: Neo4JConnection):
    pathToUserFile = os.path.join(".cached_results", f"users__{userName}.json")
    if not os.path.exists(pathToUserFile):
        print("[USER]", f"[{userName}]", "skipping user as he does not exist")
        return
    with open(pathToUserFile) as fileWithUser:
        parsedUserData = json.loads(fileWithUser.read())
        if "message" in parsedUserData and parsedUserData["message"] == "Not Found":
            print("[USER]", f"[{userName}]", "skipping user as he does not exist")
            return
        user = User(parsedUserData)
        conn.createUserOrOrg(user)
    pass
    importGists(conn, user)
    importRepositories(conn, user)


if __name__ == '__main__':
    conn = Neo4JConnection()
    with open("users_to_fetch.txt") as userNamesList:
        for userName in userNamesList:
            userName = userName.strip()
            importUserWithData(userName, conn)
