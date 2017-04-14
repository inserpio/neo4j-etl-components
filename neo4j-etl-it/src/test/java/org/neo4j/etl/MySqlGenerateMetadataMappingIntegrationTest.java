package org.neo4j.etl;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import static org.neo4j.etl.neo4j.Neo4j.NEO4J_VERSION;
import static org.neo4j.etl.provisioning.platforms.TestType.INTEGRATION;

public class MySqlGenerateMetadataMappingIntegrationTest
{
    @ClassRule
    public static final ResourceRule<Path> tempDirectory =
            new ResourceRule<>( TemporaryDirectory.temporaryDirectory() );

    @ClassRule
    public static final ResourceRule<Server> mySqlServer = new ResourceRule<>(
            ServerFixture.server(
                    "mysql-etl-test-create-csv",
                    DatabaseType.MySQL.defaultPort(),
                    MySqlScripts.startupScript(),
                    tempDirectory.get(), INTEGRATION ) );

    @ClassRule
    public static final ResourceRule<Neo4j> neo4j =
            new ResourceRule<>( Neo4jFixture.neo4j( NEO4J_VERSION, tempDirectory.get() ) );
    private static final URI NEO_TX_URI = URI.create( "http://localhost:7474/db/data/transaction/commit" );


    @BeforeClass
    public static void setUp() throws Exception
    {
        RdbmsClient client = new RdbmsClient( DatabaseType.MySQL, mySqlServer.get().ipAddress() );
        client.execute( MySqlScripts.setupDatabaseScript().value() );
    }

    @Test
    public void shouldGenerateMappingsFileSuccessfully() throws Exception
    {
        Path importToolOptions = importToolOptions();
        String mapping = createMetadataMappings( "javabase", importToolOptions );
        assertThat( mapping, containsString( "STUDENT_ID" ) );

        Path mappingFile = tempDirectory.get().resolve( "mapping.json" );

        Files.write( mappingFile, mapping.getBytes() );

        exportFromMySqlToNeo4j( importToolOptions, mappingFile );

        neo4j.get().start();

        assertFalse( neo4j.get().containsImportErrorLog( Neo4j.DEFAULT_DATABASE ) );

        String response = neo4j.get().executeHttp( NEO_TX_URI, "MATCH (p:Person)-[r]->(c:Address) " +
                "RETURN p, type(r), c" );
        List<String> usernames = JsonPath.read( response, "$.results[*].data[*].row[0].username" );

        assertThat( usernames.size(), is( 9 ) );
    }

    private static String createMetadataMappings( String database, Path importToolOptions ) throws IOException
    {
        return NeoIntegrationCli.executeMainReturnSysOut( new String[]{
                "mysql",
                "generate-metadata-mapping",
                "--host", mySqlServer.get().ipAddress(),
                "--user", RdbmsClient.Parameters.DBUser.value(),
                "--password", RdbmsClient.Parameters.DBPassword.value(),
                "--database", database,
                "--options-file", importToolOptions.toString(),
                "--relationship-name", "column",
                "--debug"} );
    }

    private static void exportFromMySqlToNeo4j( Path importToolOptions, Path mappingFile ) throws IOException
    {
        List<String> args = new ArrayList<>();
        args.addAll( Arrays.asList( "mysql", "export",
                "--host", mySqlServer.get().ipAddress(),
                "--user", RdbmsClient.Parameters.DBUser.value(),
                "--password", RdbmsClient.Parameters.DBPassword.value(),
                "--database", "javabase",
                "--import-tool", neo4j.get().binDirectory().toString(),
                "--options-file", importToolOptions.toString(),
                "--mapping-file", mappingFile.toString(),
                "--csv-directory", tempDirectory.get().toString(),
                "--destination", neo4j.get().databasesDirectory().resolve( Neo4j.DEFAULT_DATABASE ).toString(),
                "--force" ) );

//        args.add( "--debug" );
        NeoIntegrationCli.executeMainReturnSysOut( args.toArray( new String[args.size()] ) );
    }

    private static Path importToolOptions() throws IOException
    {
        Path importToolOptions = tempDirectory.get().resolve( "import-tool-options.json" );
        ObjectMapper objectMapper = new ObjectMapper();
        HashMap<Object, Object> options = new HashMap<>();
        options.put( "delimiter", "\t" );
        options.put( "quote", "`" );
        options.put( "multiline-fields", "true" );
        objectMapper.writeValue( importToolOptions.toFile(), options );
        return importToolOptions;
    }
}
