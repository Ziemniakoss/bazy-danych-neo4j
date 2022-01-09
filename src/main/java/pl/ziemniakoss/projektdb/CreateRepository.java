package pl.ziemniakoss.projektdb;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class CreateRepository {
	@Context
	public Transaction tx;

	@Context
	public GraphDatabaseService db;

	final static String USER_QUERY = "MATCH (owner {name: $userName}) WHERE 'User' IN LABELS(owner) OR 'Organization' IN LABELS(owner)";

	@Procedure(value = "dbproject.createRepository", mode = Mode.WRITE)
	@Description("Create repository for user")
	public Stream<Repository> getCodersInLanguage(@Name("userName") String userName, @Name("repoName") String repoName) {
		ensureUserExists(userName);
		ensureUserDoesntHaveRepo(userName, repoName);
		var query = USER_QUERY +
			" CREATE (r:Repository {name: $repoName, fullName: $repoFullName})," +
			" (owner)-[:OWNS]->(r)" +
			" RETURN r";
		return tx.execute(query, new HashMap<>() {
			{
				put("userName", userName);
				put("repoName", repoName);
				put("repoFullName", String.format("%s/%s", userName, repoName));
			}
		})
			.stream()
			.map(a -> a.values().stream())
			.map(Repository::new);
	}

	private void ensureUserExists(String userName) {
		final var params = new HashMap<String, Object>();
		params.put("userName", userName);
		tx.execute(USER_QUERY + " RETURN owner", params)
			.stream()
			.map(Map::values)
			.flatMap(Collection::stream)
			.findFirst()
			.orElseThrow(() -> new RuntimeException("User does not exist"));
	}

	private void ensureUserDoesntHaveRepo(String userName, String repoName) {
		var query = "MATCH (r:Repository {name: $repoName})<-[:OWNS]-(u {name: $userName}) WHERE 'User' IN LABELS(u) OR 'Organization' IN LABELS(u) RETURN r";
		var countOfReposWithSameNameAndOwner = tx.execute(query, new HashMap<>() {{
				put("userName", userName);
				put("repoName", repoName);
			}})
			.stream()
			.mapToLong(m -> m.values().size())
			.sum();
		if (countOfReposWithSameNameAndOwner > 0) {
			throw new RuntimeException("Repo with same name is already owned by this user");
		}

	}

	public static final class Repository {
		public final Object node;

		Repository(Object node) {
			this.node = node;
		}
	}
}