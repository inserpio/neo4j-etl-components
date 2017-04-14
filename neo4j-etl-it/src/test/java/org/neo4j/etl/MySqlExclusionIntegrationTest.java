package org.neo4j.etl;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.neo4j.etl.rdbms.RdbmsClient;
import org.neo4j.etl.neo4j.Neo4j;
import org.neo4j.etl.provisioning.Neo4jFixture;
import org.neo4j.etl.provisioning.Server;
import org.neo4j.etl.provisioning.ServerFixture;
import org.neo4j.etl.provisioning.scripts.MySqlScripts;
import org.neo4j.etl.sql.DatabaseType;
import org.neo4j.etl.util.ResourceRule;
import org.neo4j.etl.util.TemporaryDirectory;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import static org.neo4j.etl.neo4j.Neo4j.NEO4J_VERSION;
import static org.neo4j.etl.provisioning.platforms.TestType.INTEGRATION;

public class MySqlExclusionIntegrationTest
{
    private static final String[] tablesToExclude = {"Orphan_Table", "Yet_Another_Orphan_Table", "Table_B"};
//    private static final String[] tablesToInclude = {"Join_Table", "Table_A"};

    @ClassRule
    public static final ResourceRule<Path> tempDirectory =
            new ResourceRule<>( TemporaryDirectory.temporaryDirectory() );

    @ClassRule
    public static final ResourceRule<Server> mySqlServer = new ResourceRule<>(
            ServerFixture.server(
                    "mysql-etl-test",
                    DatabaseType.MySQL.defaultPort(),
                    MySqlScripts.startupScript(),
                    tempDirectory.get(), INTEGRATION ) );

    @ClassRule
    public static final ResourceRule<Neo4j> neo4j = new ResourceRule<>(
            Neo4jFixture.neo4j( NEO4J_VERSION, tempDirectory.get() ) );
    private static final URI NEO_TX_URI = URI.create( "http://localhost:7474/db/data/transaction/commit" );

    @BeforeClass
    public static void setUp() throws Exception
    {
        RdbmsClient client = new RdbmsClient( DatabaseType.MySQL, mySqlServer.get().ipAddress() );
        client.execute( MySqlScripts.exclusionScript().value() );
        exportFromMySqlToNeo4j( );
        neo4j.get().start();
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        neo4j.get().stop();
    }

    @Test
    public void shouldExcludeOrphanTables() throws Exception
    {
        assertFalse( neo4j.get().containsImportErrorLog( Neo4j.DEFAULT_DATABASE ) );

        String response = neo4j.get().executeHttp( NEO_TX_URI, "MATCH (lt:OrphanTable) RETURN lt" );
        List<String> leaves = JsonPath.read( response, "$.results[*].data[*].row[0]" );
        assertThat( leaves.size(), is( 0 ) );

        response = neo4j.get().executeHttp( NEO_TX_URI, "MATCH (lt:YetAnotherOrphanTable) RETURN lt" );
        leaves = JsonPath.read( response, "$.results[*].data[*].row[0]" );
        assertThat( leaves.size(), is( 0 ) );
    }

    @Test
    public void shouldExcludeTargetTableButNotJoinTable() throws Exception
    {
        assertFalse( neo4j.get().containsImportErrorLog( Neo4j.DEFAULT_DATABASE ) );

        String response = neo4j.get().executeHttp( NEO_TX_URI, "MATCH (lt:TableB) RETURN lt" );
        List<String> leaves = JsonPath.read( response, "$.results[*].data[*].row[0]" );
        assertThat( leaves.size(), is( 0 ) );

        response = neo4j.get().executeHttp( NEO_TX_URI, "MATCH (lt:JoinTable) RETURN lt" );
        leaves = JsonPath.read( response, "$.results[*].data[*].row[0]" );
        assertThat( leaves.size(), is( 1 ) );
    }

    private static void exportFromMySqlToNeo4j( ) throws IOException
    {
        Path importToolOptions = tempDirectory.get().resolve( "import-tool-options.json" );
        HashMap<Object, Object> options = new HashMap<>();
        ObjectMapper objectMapper = new ObjectMapper();
        List<String> args = new ArrayList<String>(  );

        options.put( "quote", "`" );
        options.put( "delimiter", "\t" );
        options.put( "multiline-fields", "true" );
        objectMapper.writeValue( importToolOptions.toFile(), options );

        args.addAll( Arrays.asList( "mysql", "export",
                "--host", mySqlServer.get().ipAddress(),
                "--user", RdbmsClient.Parameters.DBUser.value(),
                "--password", RdbmsClient.Parameters.DBPassword.value(),
                "--database", "exclusion",
                "--import-tool", neo4j.get().binDirectory().toString(),
                "--options-file", importToolOptions.toString(),
                "--csv-directory", tempDirectory.get().toString(),
                "--destination", neo4j.get().databasesDirectory().resolve( Neo4j.DEFAULT_DATABASE ).toString(),
                "--force", "--debug",
                "--exc", "exclude" ) );

        args.addAll( Arrays.asList( tablesToExclude ) );

        NeoIntegrationCli.executeMainReturnSysOut( args.toArray( new String[args.size()] ) );
    }
}
