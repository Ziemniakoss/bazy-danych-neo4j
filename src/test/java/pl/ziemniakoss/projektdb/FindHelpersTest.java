package pl.ziemniakoss.projektdb;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.Config;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FindHelpersTest {

	private static final Config driverConfig = Config.builder().withoutEncryption().build();
	private Neo4j embeddedDatabaseServer;

	@BeforeAll
	void initializeNeo4j() {
		this.embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
			.withDisabledServer()
			.withProcedure(FindHelpers.class)
			.build();
		try(
			var driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
			var session = driver.session()
		) {
			session.run(
				"CREATE (ow:User {name: 'owner'})," +
					"(lang:Language {name: 'Rust'})," +
					"(repo:Repository)," +
					"(contr:User {name: 'contr'})," +
					"(ow)-[:OWNS]->(repo)," +
					"(contr)-[:CONTRIBUTES]->(repo)," +
					"(repo)-[:IS_WRITTEN_IN]->(lang)"
			);
		}
	}

	@Test
	void findHelpers() {
		try (
			var driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
			var session = driver.session()
		) {
			var result = session.run("call dbproject.findHelpers('owner')").list();
			assertThat(result.size()).isEqualTo(1);
		}
	}
}