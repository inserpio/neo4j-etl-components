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
import org.neo4j.etl.provisioning.scripts.PostgreSqlScripts;
import org.neo4j.etl.rdbms.RdbmsClient;
import org.neo4j.etl.sql.DatabaseType;
import org.neo4j.etl.util.ResourceRule;
import org.neo4j.etl.util.TemporaryDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.neo4j.etl.neo4j.Neo4j.NEO4J_VERSION;
import static org.neo4j.etl.neo4j.Neo4j.NEO_TX_URI;
import static org.neo4j.etl.provisioning.platforms.TestType.INTEGRATION;

public class PostgreSqlNorthWindDatabaseInspectorIntegrationTest
{
    @ClassRule
    public static final ResourceRule<Path> tempDirectory =
            new ResourceRule<>( TemporaryDirectory.temporaryDirectory() );

    @ClassRule
    public static final ResourceRule<Server> postgreSqlServer = new ResourceRule<>(
            ServerFixture.server(
                    "postgresql-etl-test-nw",
                    5433,
                    PostgreSqlScripts.startupScript(),
                    tempDirectory.get(), INTEGRATION ) );

    @ClassRule
    public static final ResourceRule<Neo4j> neo4j = new ResourceRule<>(
            Neo4jFixture.neo4j( NEO4J_VERSION, tempDirectory.get() ) );

    @BeforeClass
    public static void setUp() throws Exception
    {
        RdbmsClient client = new RdbmsClient(
                DatabaseType.PostgreSQL,
                postgreSqlServer.get().ipAddress(),
                5433,
                "northwind",
                RdbmsClient.Parameters.DBUser.value(),
                RdbmsClient.Parameters.DBPassword.value() );
        //client.execute( PostgreSqlScripts.northwindScript().value() );
        exportFromPostreSqlToNeo4j( "northwind" );
        neo4j.get().start();
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        neo4j.get().stop();
    }

    @Test
    public void shouldExportFromPostgreSqlAndImportIntoGraph() throws Exception
    {
        assertFalse( neo4j.get().containsImportErrorLog( Neo4j.DEFAULT_DATABASE ) );

        String customersJson = neo4j.get().executeHttp( NEO_TX_URI, "MATCH (c:Customer) RETURN c" );
        String customersWithOrdersJson = neo4j.get().executeHttp( NEO_TX_URI,
                "MATCH (c)--(o) " +
                        "WHERE (c:Customer)<-[:CUSTOMER]-(o:Order) RETURN DISTINCT c" );
        List<String> customers = JsonPath.read( customersJson, "$.results[*].data[*].row[0]" );
        List<String> customersWithOrders = JsonPath.read( customersWithOrdersJson, "$.results[*].data[*].row[0]" );
        assertThat( customers.size(), is( 93 ) );
        assertThat( customersWithOrders.size(), is( 89 ) );

        String newOrdersJson = neo4j.get().executeHttp( NEO_TX_URI,
                "MATCH (c)--(o) " +
                        "WHERE (c:Customer)--(o:Order{shipCity:'Lyon'}) RETURN DISTINCT c" );
        List<String> customersWithOrdersToLyon = JsonPath.read( newOrdersJson, "$.results[*].data[*].row[0]" );

        assertThat( customersWithOrdersToLyon.size(), is( 1 ) );

        String territoriesWithEmployeesResponse = neo4j.get().executeHttp( NEO_TX_URI,
                "MATCH (e:Employee)-->(t:Territory) RETURN e,t;" );
        List<String> territories = JsonPath.read( territoriesWithEmployeesResponse, "$.results[*].data[*].row[1]" );

        assertThat( territories.size(), is( 49 ) );

        String managers = neo4j.get().executeHttp( NEO_TX_URI,
                "MATCH (e:Employee)--(r:Employee) WHERE NOT (e:Employee)-->(r:Employee) RETURN e;" );
        List<String> lastNames = JsonPath.read( managers, "$.results[*].data[*].row[0].lastName" );
        assertThat( lastNames, hasItems( "Fuller", "Buchanan" ) );
    }

    private static void exportFromPostreSqlToNeo4j( String database ) throws IOException
    {
        Path importToolOptions = tempDirectory.get().resolve( "import-tool-options.json" );
        ObjectMapper objectMapper = new ObjectMapper();
        HashMap<Object, Object> options = new HashMap<>();
        options.put( "multiline-fields", "true" );
        objectMapper.writeValue( importToolOptions.toFile(), options );

        NeoIntegrationCli.executeMainReturnSysOut(
                new String[]{"postgresql",
                        "export",
                        "--host", postgreSqlServer.get().ipAddress(),
                        "--port", "5433",
                        "--user", RdbmsClient.Parameters.DBUser.value(),
                        "--password", RdbmsClient.Parameters.DBPassword.value(),
                        "--database", database,
                        "--import-tool", neo4j.get().binDirectory().toString(),
                        "--options-file", importToolOptions.toString(),
                        "--csv-directory", tempDirectory.get().toString(),
                        "--destination", neo4j.get().databasesDirectory().resolve( Neo4j.DEFAULT_DATABASE ).toString(),
                        "--delimiter", "\t",
                        "--quote", "`",
                        "--force"} );
    }
}
