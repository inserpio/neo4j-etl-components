package org.neo4j.etl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.*;
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
import java.util.HashMap;

import static java.lang.String.format;
import static org.neo4j.etl.neo4j.Neo4j.NEO4J_VERSION;
import static org.neo4j.etl.provisioning.platforms.TestType.INTEGRATION;

public class PostgreSqlNorthWindCypherShellIntegrationTest
{
    @ClassRule
    public static final ResourceRule<Path> tempDirectory =
            new ResourceRule<>( TemporaryDirectory.temporaryDirectory() );

    @ClassRule
    public static final ResourceRule<Server> postgreSqlServer= new ResourceRule<>(
            ServerFixture.server("postgresql-etl-test-nw", 5433,
                    RdbmsScripts.startupScript( "PostgreSQL" ),
                    tempDirectory.get(), INTEGRATION ) );

    @ClassRule
    public static final ResourceRule<Neo4j> neo4j = new ResourceRule<>(
            Neo4jFixture.neo4j( NEO4J_VERSION, tempDirectory.get() ) );

    @BeforeClass
    public static void setUp() throws Exception
    {
        neo4j.get().start();

        PostgreSqlNorthWindIntegrationTest.setUp( postgreSqlServer, neo4j, exportFromPostreSqlToNeo4j() );
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        PostgreSqlNorthWindIntegrationTest.tearDown( neo4j );
    }

    @Ignore
    @Test
    public void shouldExportFromPostgreSqlAndImportIntoGraph() throws Exception
    {
        PostgreSqlNorthWindIntegrationTest.shouldExportFromPostgreSqlAndImportIntoGraph( neo4j );
    }

    protected static String[] exportFromPostreSqlToNeo4j() throws IOException
    {
        Path importToolOptions = tempDirectory.get().resolve( "import-tool-options.json" );
        ObjectMapper objectMapper = new ObjectMapper();
        HashMap<Object, Object> options = new HashMap<>();
        options.put( "multiline-fields", "true" );
        objectMapper.writeValue( importToolOptions.toFile(), options );

        return new String[] {
                "export",
                "--rdbms:url", format( "jdbc:postgresql://%s:%s/%s?ssl=false", postgreSqlServer.get().ipAddress(), "5433", "northwind" ),
                "--rdbms:user", RdbmsClient.Parameters.DBUser.value(),
                "--rdbms:password", RdbmsClient.Parameters.DBPassword.value(),
                "--using", "cypher:shell",
                "--graph:neo4j:url", "bolt://localhost:7687",
                "--graph:neo4j:user", "neo4j",
                "--graph:neo4j:password", "neo4j",
                "--import-tool", neo4j.get().binDirectory().toString(),
                "--options-file", importToolOptions.toString(),
                "--csv-directory", neo4j.get().importDirectory().toString(),
                "--destination", neo4j.get().databasesDirectory().resolve( Neo4j.DEFAULT_DATABASE ).toString(),
                "--force"
        };
    }
}
