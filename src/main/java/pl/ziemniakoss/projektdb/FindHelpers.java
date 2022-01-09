package pl.ziemniakoss.projektdb;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class FindHelpers {
	@Context
	public Transaction tx;

	@Procedure(value = "dbproject.findHelpers", mode = Mode.READ)
	@Description("Find users that contribute to repositories of given user")
	public Stream<Coder> getCodersInLanguage(@Name("userName") String userName) {
		final String query =
			"MATCH " +
				"    (c)-[:CONTRIBUTES]-> " +
				"        (repos:Repository) " +
				"    <-[:OWNS]-(sf {name: $userName})" +
				"RETURN c";
		final var params = new HashMap<String, Object>();
		params.put("userName", userName);
		return tx.execute(query, params)
			.stream()
			.map(Map::values)
			.flatMap(Collection::stream)
			.map(a -> (Node) a)
			.map(Coder::new);
	}

	public static final class Coder {
		public final Node node;

		Coder(Node node) {
			this.node = node;
		}
	}

}
