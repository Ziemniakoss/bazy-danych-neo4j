package pl.ziemniakoss.projektdb;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class FindCodersInLanguage {
	@Context
	public Transaction tx;

	@Procedure(value = "dbproject.findCodersInLanguage", mode = Mode.READ)
	@Description("Find every coder that contributes to or owns repository in given language")
	public Stream<Coder> getCodersInLanguage(@Name("language") String language) {
		final String query =
			"MATCH " +
			"    (repos:Repository)-[:IS_WRITTEN_IN]->" +
			"        (:Language {name: $languageName}), " +
			"    (contributors)-[:CONTRIBUTES]->(repos), " +
			"    (owners)-[:OWNS]->(repos) " +
			"RETURN owners, contributors";
		final var params = new HashMap<String, Object>();
		params.put("languageName", language);
		return tx.execute(query, params)
			.stream()
			.map(Map::values)
			.flatMap(Collection::stream)
			.map(a->(Node) a)
			.map(Coder::new);
	}

	public static final class Coder {
		public final Node node;

		Coder(Node node) {
			this.node = node;
		}
	}

}
