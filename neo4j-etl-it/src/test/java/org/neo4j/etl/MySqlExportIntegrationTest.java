package org.neo4j.etl;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.neo4j.etl.provisioning.scripts.RdbmsScripts;
import org.neo4j.etl.sql.DatabaseType;
import org.neo4j.etl.util.ResourceRule;
import org.neo4j.etl.util.TemporaryDirectory;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import static org.neo4j.etl.neo4j.Neo4j.NEO4J_VERSION;
import static org.neo4j.etl.neo4j.Neo4j.NEO_TX_URI;
import static org.neo4j.etl.provisioning.platforms.TestType.INTEGRATION;

public class MySqlExportIntegrationTest
{
    @ClassRule
    public static final ResourceRule<Path> tempDirectory =
            new ResourceRule<>( TemporaryDirectory.temporaryDirectory() );

    @ClassRule
    public static final ResourceRule<Server> mySqlServer = new ResourceRule<>(
            ServerFixture.server(
                    "mysql-etl-test-export",
                    DatabaseType.MySQL.defaultPort(),
                    RdbmsScripts.startupScript( DatabaseType.MySQL ),
                    tempDirectory.get(), INTEGRATION ) );

    @ClassRule
    public static final ResourceRule<Neo4j> neo4j = new ResourceRule<>(
            Neo4jFixture.neo4j( NEO4J_VERSION, tempDirectory.get() ) );

    @BeforeClass
    public static void setUp() throws Exception
    {
        RdbmsClient client = new RdbmsClient( DatabaseType.MySQL, mySqlServer.get().ipAddress() );
        client.execute( RdbmsScripts.setupDatabaseScript( DatabaseType.MySQL ).value() );
        exportFromMySqlToNeo4j();
        neo4j.get().start();
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        neo4j.get().stop();
    }

    @Test
    public void shouldExportFromMySqlAndImportIntoGraph() throws Exception
    {
        // then
        assertFalse( neo4j.get().containsImportErrorLog( Neo4j.DEFAULT_DATABASE ) );

        String response = neo4j.get().executeHttp( NEO_TX_URI, "MATCH (p:Person)-[r]->(c:Address) RETURN p, type" +
                "(r), c" );
        List<String> usernames = JsonPath.read( response, "$.results[*].data[*].row[0].username" );
        List<String> relationships = JsonPath.read( response, "$.results[*].data[*].row[1]" );
        List<String> postcodes = JsonPath.read( response, "$.results[*].data[*].row[2].postcode" );

        assertThat( usernames.size(), is( 9 ) );

        assertThat( usernames, hasItems(
                "user-1", "user-2", "user-3", "user-4", "user-5", "user-6", "user-7", "user-8", "user-9" ) );
        assertEquals( asList( "ADDRESS", "ADDRESS", "ADDRESS", "ADDRESS", "ADDRESS", "ADDRESS",
                "ADDRESS", "ADDRESS", "ADDRESS" ), relationships );
        assertThat( postcodes, hasItems( "AB12 1XY", "XY98 9BA", "ZZ1 0MN" ) );
    }

    @Test
    public void shouldExportTableWithCompositeJoinColumns() throws Exception
    {
        assertFalse( neo4j.get().containsImportErrorLog( Neo4j.DEFAULT_DATABASE ) );

        String response = neo4j.get().executeHttp( NEO_TX_URI,
                "MATCH (p:Book)-[r]->(c:Author) RETURN p, type(r),c" );
        List<String> books = JsonPath.read( response, "$.results[*].data[*].row[0].name" );
        List<String> relationships = JsonPath.read( response, "$.results[*].data[*].row[1]" );
        List<String> lastNames = JsonPath.read( response, "$.results[*].data[*].row[2].lastName" );

        assertThat( books.size(), is( 3 ) );

        assertThat( books, hasItems( "Database System Concepts", "Database Management Systems", "Computer Networks" ) );
        assertEquals( asList( "AUTHOR", "AUTHOR", "AUTHOR" ), relationships );
        assertEquals( asList( "Silberschatz", "Tanenbaum", "Ramakrishnan" ), lastNames );
    }

    @Test
    public void shouldExportTableWithSelfJoins() throws Exception
    {
        assertFalse( neo4j.get().containsImportErrorLog( Neo4j.DEFAULT_DATABASE ) );

        String response = neo4j.get().executeHttp( NEO_TX_URI,
                "MATCH (b1:Book)-[r]->(b2:Book) RETURN b1, type(r),b2" );
        List<String> books = JsonPath.read( response, "$.results[*].data[*].row[0].name" );
        List<String> relationships = JsonPath.read( response, "$.results[*].data[*].row[1]" );
        List<String> referencedBook = JsonPath.read( response, "$.results[*].data[*].row[2].name" );

        assertThat( books.size(), is( 1 ) );

        assertThat( books, hasItems( "Database Management Systems" ) );
        assertEquals( singletonList( "BOOK" ), relationships );
        assertThat( referencedBook, hasItems( "Database System Concepts" ) );
    }

    @Test
    public void shouldExportFromMySqlAndImportIntoGraphForNumericAndStringTables() throws Exception
    {
        assertFalse( neo4j.get().containsImportErrorLog( Neo4j.DEFAULT_DATABASE ) );

        String response = neo4j.get().executeHttp( NEO_TX_URI,
                "MATCH (p:StringTable)-[r]->(c:NumericTable) RETURN p, c" );

        List<Map<String, String>> stringFields = JsonPath.read( response, "$.results[*].data[0].row[0]" );
        List<Map<String, Object>> numericFields = JsonPath.read( response, "$.results[*].data[0].row[1]" );

        assertThat( stringFields.get( 0 ).values(), hasItems(
                "val-1", "mediumtext_field", "tinytext_field",
                "char-field", "text_field", "varchar-field", "longtext_field" ) );
        assertThat( numericFields.get( 0 ).values(), hasItems( 123, 123, 123.2, 123, 18.10, 1.232343445E7, 2, 1 ) );
    }

    @Test
    public void shouldExportFromMySqlAndImportIntoGraphForNumericAndDateTables() throws Exception
    {
        assertFalse( neo4j.get().containsImportErrorLog( Neo4j.DEFAULT_DATABASE ) );

        String response = neo4j.get().executeHttp( NEO_TX_URI,
                "MATCH (p:DateTable)-[r]->(c:NumericTable) RETURN p, c" );

        List<Map<String, Object>> dateFields = JsonPath.read( response, "$.results[*].data[0].row[0]" );
        List<Map<String, Object>> numericFields = JsonPath.read( response, "$.results[*].data[0].row[1]" );
        assertThat( dateFields.get( 0 ).values(), hasItems(
                "22:34:35",
                "1987-01-01",
                "1989-01-23 00:00:00.0",
                "2038-01-19 03:14:07.0",
                "1988-01-23" ) );
        assertThat( numericFields.get( 0 ).size(), is( 8 ) );
        assertThat( numericFields.get( 0 ).values(), hasItems( 123, 123, 123.2, 123, 18.10, 1.232343445E7, 2, 1 ) );
    }

    @Test
    public void shouldExportFromMySqlAndImportIntoGraphForThreeTableJoinWithProperties() throws Exception
    {
        assertFalse( neo4j.get().containsImportErrorLog( Neo4j.DEFAULT_DATABASE ) );

        String response = neo4j.get()
                .executeHttp( NEO_TX_URI, "MATCH (p:Student)<-[r]-(c:Course) RETURN p, c, r.credits" );

        List<String> students = JsonPath.read( response, "$.results[*].data[*].row[0].username" );
        List<String> courses = JsonPath.read( response, "$.results[*].data[*].row[1].name" );
        List<Integer> credits = JsonPath.read( response, "$.results[*].data[*].row[2]" );

        assertThat( students.size(), is( 4 ) );
        assertThat( students, hasItems( "jim", "mark" ) );


        assertThat( courses.size(), is( 4 ) );
        assertThat( courses, hasItems( "Science", "Maths", "English" ) );

        assertThat( credits, hasItems( 1, 2, 3, 4 ) );

        String coursesResponse = neo4j.get().executeHttp( NEO_TX_URI, "MATCH (c:Course) RETURN c.name" );
        String studentsResponse = neo4j.get().executeHttp( NEO_TX_URI, "MATCH (s:Student) RETURN s.username" );

        List<String> allCourses = JsonPath.read( coursesResponse, "$.results[*].data[*].row[0]" );
        List<String> allStudents = JsonPath.read( studentsResponse, "$.results[*].data[*].row[0]" );

        assertThat( allStudents, hasItems( "jim", "mark", "eve" ) );
        assertThat( allCourses, hasItems( "Science", "Maths", "English", "Theology" ) );
    }

    @Test
    public void shouldExportFromMySqlAndImportIntoGraphForCompositeThreeTableJoinWithProperties() throws Exception
    {
        assertFalse( neo4j.get().containsImportErrorLog( Neo4j.DEFAULT_DATABASE ) );

        String response = neo4j.get()
                .executeHttp( NEO_TX_URI, "MATCH (a:Author)-[r]->(p:Publisher) RETURN a, p" );

        List<String> authors = JsonPath.read( response, "$.results[*].data[*].row[0].lastName" );
        List<String> publishers = JsonPath.read( response, "$.results[*].data[*].row[1].name" );

        assertThat( authors.size(), is( 2 ) );
        assertThat( authors, hasItems( "Tanenbaum", "Silberschatz" ) );

        assertThat( publishers.size(), is( 2 ) );
        assertThat( publishers, hasItems( "Pearson", "O'Reilly" ) );
    }

    @Test
    public void shouldExportTableWithMoreThanTwoForeignKeysAndNoPrimaryKeyAsAnIntermediateNode() throws Exception
    {
        assertFalse( neo4j.get().containsImportErrorLog( Neo4j.DEFAULT_DATABASE ) );

        String response = neo4j.get().executeHttp(
                NEO_TX_URI,
                "MATCH (t:Team)-[:STUDENT]-(s:Student) " +
                        "RETURN t.name AS team, s.username AS student ORDER BY student" );

        List<String> teams = JsonPath.read( response, "$.results[*].data[*].row[0]" );
        List<String> students = JsonPath.read( response, "$.results[*].data[*].row[1]" );

        assertThat( teams, hasItems( "Rassilon" ) );

        assertThat( students.size(), is( 3 ) );
        assertEquals( students, asList( "eve", "jim", "mark" ) );
    }

    private static void exportFromMySqlToNeo4j() throws IOException
    {
        Path importToolOptions = tempDirectory.get().resolve( "import-tool-options.json" );
        HashMap<Object, Object> options = new HashMap<>();
        ObjectMapper objectMapper = new ObjectMapper();
        List<String> args = new ArrayList<>();

        options.put( "quote", "`" );
        options.put( "delimiter", "\t" );
        options.put( "multiline-fields", "true" );
        objectMapper.writeValue( importToolOptions.toFile(), options );

        args.addAll( Arrays.asList( "mysql", "export",
                "--host", mySqlServer.get().ipAddress(),
                "--user", RdbmsClient.Parameters.DBUser.value(),
                "--password", RdbmsClient.Parameters.DBPassword.value(),
                "--database", "javabase",
                "--import-tool", neo4j.get().binDirectory().toString(),
                "--options-file", importToolOptions.toString(),
                "--csv-directory", tempDirectory.get().toString(),
                "--destination", neo4j.get().databasesDirectory().resolve( Neo4j.DEFAULT_DATABASE ).toString(),
                "--force" ) );

        args.add( "--debug" );

        NeoIntegrationCli.executeMainReturnSysOut( args.toArray( new String[args.size()] ) );
    }
}
