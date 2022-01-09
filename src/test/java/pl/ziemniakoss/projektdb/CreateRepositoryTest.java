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
public class CreateRepositoryTest {

	private static final Config driverConfig = Config.builder().withoutEncryption().build();
	private Neo4j embeddedDatabaseServer;

	@BeforeAll
	void initializeNeo4j() {
		this.embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
			.withDisabledServer()
			.withProcedure(CreateRepository.class)
			.build();
	}

	@Test
	void createRepository_userDoesntExist() {
		try (
			var driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
			var session = driver.session()
		) {
			session.run("call dbproject.createRepository('testUserA', 'testRepo')");
		} catch (RuntimeException e) {
			assertThat(e.getMessage()).contains("User does not exist");
		}
	}

	@Test
	void createRepository_repoAlreadyExists() {
		try (
			var driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
			var session = driver.session()
		) {
			session.run("CREATE (u:User {name: 'testUserB'}), (r:Repository {name: 'testRepo'})");
				session.run("call dbproject.createRepository('testUserB', 'testRepo')");
		}catch(RuntimeException e) {
			assertThat(e.getMessage()).contains("Repo with same name is already owned by this user");
		}
	}

	@Test
	void createRepository_createForUser() {
		try (
			var driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
			var session = driver.session()
		) {
			session.run("CREATE (u:User {name: 'testUserC'})");
			var result = session.run("call dbproject.createRepository('testUserC', 'testRepo')").list();
			assertThat(result.size()).isEqualTo(1);
		}
	}
}