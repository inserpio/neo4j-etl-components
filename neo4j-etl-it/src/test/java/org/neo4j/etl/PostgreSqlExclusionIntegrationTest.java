package org.neo4j.etl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.etl.neo4j.Neo4j;
import org.neo4j.etl.provisioning.Neo4jFixture;
import org.neo4j.etl.provisioning.Server;
import org.neo4j.etl.provisioning.ServerFixture;
import org.neo4j.etl.provisioning.scripts.RdbmsScripts;
import org.neo4j.etl.rdbms.RdbmsClient;
import org.neo4j.etl.util.ResourceRule;
import org.neo4j.etl.util.TemporaryDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.etl.neo4j.Neo4j.NEO4J_VERSION;
import static org.neo4j.etl.neo4j.Neo4j.NEO_TX_URI;
import static org.neo4j.etl.provisioning.platforms.TestType.INTEGRATION;

public class PostgreSqlExclusionIntegrationTest {
    @ClassRule
    public static final ResourceRule<Path> tempDirectory =
            new ResourceRule<>(TemporaryDirectory.temporaryDirectory());
    //    private static final String[] tablesToInclude = {"Join_Table", "Table_A"};
    @ClassRule
    public static final ResourceRule<Server> postgreSqlServer = new ResourceRule<>(
            ServerFixture.server(
                    "postgresql-etl-test",
                    5433,
                    RdbmsScripts.startupScript("PostgreSQL"),
                    tempDirectory.get(), INTEGRATION));
    @ClassRule
    public static final ResourceRule<Neo4j> neo4j = new ResourceRule<>(
            Neo4jFixture.neo4j(NEO4J_VERSION, tempDirectory.get()));
    private static final String[] tablesToExclude = {"Orphan_Table", "Yet_Another_Orphan_Table", "Table_B"};
    private static String url;

    @BeforeClass
    public static void setUp() throws Exception {
        url = String.format("jdbc:postgresql://%s:%s/%s?ssl=false", postgreSqlServer.get().ipAddress(), 5433, "exclusion");

        String adminUrl = String.format("jdbc:postgresql://%s:%s/%s?ssl=false", postgreSqlServer.get().ipAddress(), 5433, "postgres");
        RdbmsClient postgres = new RdbmsClient(
                adminUrl,
                RdbmsClient.Parameters.DBUser.value(),
                RdbmsClient.Parameters.DBPassword.value());
        postgres.execute(RdbmsScripts.exclusionStartupScript("PostgreSQL").value());

        RdbmsClient exclusion = new RdbmsClient(
                url,
                RdbmsClient.Parameters.DBUser.value(),
                RdbmsClient.Parameters.DBPassword.value());
        exclusion.execute(RdbmsScripts.exclusionScript("PostgreSQL").value());

        exportFromPostgreSqlToNeo4j();

        neo4j.get().start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        neo4j.get().stop();
    }

    private static void exportFromPostgreSqlToNeo4j() throws IOException {
        Path importToolOptions = tempDirectory.get().resolve("import-tool-options.json");
        HashMap<Object, Object> options = new HashMap<>();
        ObjectMapper objectMapper = new ObjectMapper();
        List<String> args = new ArrayList<String>();

        options.put("quote", "'");
        options.put("delimiter", "\t");
        options.put("multiline-fields", "true");
        objectMapper.writeValue(importToolOptions.toFile(), options);

        args.addAll(Arrays.asList("export",
                "--rdbms:url", url,
                "--rdbms:user", RdbmsClient.Parameters.DBUser.value(),
                "--rdbms:password", RdbmsClient.Parameters.DBPassword.value(),
                "--import-tool", neo4j.get().binDirectory().toString(),
                "--options-file", importToolOptions.toString(),
                "--csv-directory", tempDirectory.get().toString(),
                "--destination", neo4j.get().databasesDirectory().resolve(Neo4j.DEFAULT_DATABASE).toString(),
                "--force", "--debug",
                "--exc", "exclude"));

        args.addAll(Arrays.asList(tablesToExclude));

        NeoIntegrationCli.executeMainReturnSysOut(args.toArray(new String[args.size()]));
    }

    @Test
    public void shouldExcludeOrphanTables() throws Exception {
        assertFalse(neo4j.get().containsImportErrorLog(Neo4j.DEFAULT_DATABASE));

        String response = neo4j.get().executeHttp(NEO_TX_URI, "MATCH (lt:OrphanTable) RETURN lt");
        List<String> leaves = JsonPath.read(response, "$.results[*].data[*].row[0]");
        assertThat(leaves.size(), is(0));

        response = neo4j.get().executeHttp(NEO_TX_URI, "MATCH (lt:YetAnotherOrphanTable) RETURN lt");
        leaves = JsonPath.read(response, "$.results[*].data[*].row[0]");
        assertThat(leaves.size(), is(0));
    }

    @Test
    public void shouldExcludeTargetTableButNotJoinTable() throws Exception {
        assertFalse(neo4j.get().containsImportErrorLog(Neo4j.DEFAULT_DATABASE));

        String response = neo4j.get().executeHttp(NEO_TX_URI, "MATCH (lt:TableB) RETURN lt");
        List<String> leaves = JsonPath.read(response, "$.results[*].data[*].row[0]");
        assertThat(leaves.size(), is(0));

        response = neo4j.get().executeHttp(NEO_TX_URI, "MATCH (lt:JoinTable) RETURN lt");
        leaves = JsonPath.read(response, "$.results[*].data[*].row[0]");
        assertThat(leaves.size(), is(1));
    }

}
