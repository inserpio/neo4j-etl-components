package org.neo4j.etl.commands;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.etl.io.AwaitHandle;
import org.neo4j.etl.sql.DatabaseClient;
import org.neo4j.etl.sql.QueryResults;
import org.neo4j.etl.sql.StubQueryResults;
import org.neo4j.etl.sql.metadata.*;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Ignore
public class DatabaseInspectorTest
{
    private DatabaseClient databaseClient = mock( DatabaseClient.class );

    @Test
    public void shouldExportTablesAndJoinsForTwoTableJoin() throws Exception
    {
        // given
        QueryResults personTableSchema = StubQueryResults.builder()
                .columns( "COLUMN_NAME", "REFERENCED_TABLE_NAME", "COLUMN_KEY" )
                .addRow( "id", null, "PRI" )
                .addRow( "username", null, "Data" )
                .addRow( "addressId", "Address", "MUL" )
                .build();


        QueryResults personResults = StubQueryResults.builder()
                .columns( "COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE" )
                .addRow( "id", "INT", "PrimaryKey" )
                .addRow( "username", "TEXT", "Data" )
                .addRow( "addressId", "int", "ForeignKey" )
                .build();

        QueryResults addressTableSchema = StubQueryResults.builder()
                .columns( "COLUMN_NAME", "REFERENCED_TABLE_NAME", "COLUMN_KEY" )
                .addRow( "id", null, "PRI" )
                .addRow( "postcode", null, "Data" )
                .build();

        QueryResults addressResults = StubQueryResults.builder()
                .columns( "COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE" )
                .addRow( "id", "INT", "PrimaryKey" )
                .addRow( "postcode", "TEXT", "Data" )
                .build();

        QueryResults joinResults = StubQueryResults.builder()
                .columns( "SOURCE_TABLE_SCHEMA",
                        "SOURCE_TABLE_NAME",
                        "SOURCE_COLUMN_NAME",
                        "SOURCE_COLUMN_TYPE",
                        "TARGET_TABLE_SCHEMA",
                        "TARGET_TABLE_NAME",
                        "TARGET_COLUMN_NAME",
                        "TARGET_COLUMN_TYPE" )
                .addRow( "test", "Person", "id", "PrimaryKey", "test", "Person", "id", "PrimaryKey" )
                .addRow( "test", "Person", "addressId", "ForeignKey", "test", "Address", "id", "PrimaryKey" )
                .build();


        TableName person = new TableName( "test.Person" );
        TableName address = new TableName( "test.Address" );

        when( databaseClient.tables( Schema.UNDEFINED ) ).thenReturn( asList( person, address ) );
        when( databaseClient.executeQuery( any( String.class ) ) )
                .thenReturn( AwaitHandle.forReturnValue( personTableSchema ) )
                .thenReturn( AwaitHandle.forReturnValue( personResults ) )
                .thenReturn( AwaitHandle.forReturnValue( joinResults ) )
                .thenReturn( AwaitHandle.forReturnValue( addressTableSchema ) )
                .thenReturn( AwaitHandle.forReturnValue( addressResults ) );

        // when
        DatabaseInspector databaseInspector = new DatabaseInspector( databaseClient );
        SchemaExport schemaExport = databaseInspector.buildSchemaExport( Schema.UNDEFINED );

        // then
        List<TableName> tableNames = schemaExport.tables().stream().map( Table::name ).collect( Collectors.toList() );
        Join join = schemaExport.joins().stream().findFirst().get();


        assertThat( tableNames, matchesCollection( asList( person, address ) ) );
        assertThat( join.tableNames(), hasItems( person, address ) );
        assertEquals( asList( "test.Person.addressId",
                        "test.Address.id",
                        "test.Person.id",
                        "test.Person.id" ),
                keyNames( join ) );
        assertTrue( schemaExport.joinTables().isEmpty() );
    }

    @Test
    public void shouldExportTablesAndJoinsForTwoTableJoinUsingCompositeKey() throws Exception
    {
        // given
        QueryResults bookSchema = StubQueryResults.builder()
                .columns( "COLUMN_NAME", "REFERENCED_TABLE_NAME", "COLUMN_KEY" )
                .addRow( "id", null, "PRI" )
                .addRow( "name", null, "Data" )
                .addRow( "author_first_name", "Author", "MUL" )
                .addRow( "author_last_name", "Author", "MUL" )
                .build();


        QueryResults bookResults = StubQueryResults.builder()
                .columns( "COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE" )
                .addRow( "id", "INT", "PrimaryKey" )
                .addRow( "name", "TEXT", "Data" )
                .addRow( "author_first_name", "TEXT", "ForeignKey" )
                .addRow( "author_last_name", "TEXT", "ForeignKey" )
                .build();

        QueryResults authorTableSchema = StubQueryResults.builder()
                .columns( "COLUMN_NAME", "REFERENCED_TABLE_NAME", "COLUMN_KEY" )
                .addRow( "first_name", null, "PRI" )
                .addRow( "last_name", null, "PRI" )
                .addRow( "age", null, "INT" )
                .build();

        QueryResults addressResults = StubQueryResults.builder()
                .columns( "COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE" )
                .addRow( "first_name", "TEXT", "PrimaryKey" )
                .addRow( "last_name", "TEXT", "PrimaryKey" )
                .addRow( "age", "INT", "Data" )
                .build();

        QueryResults joinResults = StubQueryResults.builder()
                .columns( "SOURCE_TABLE_SCHEMA",
                        "SOURCE_TABLE_NAME",
                        "SOURCE_COLUMN_NAME",
                        "SOURCE_COLUMN_TYPE",
                        "TARGET_TABLE_SCHEMA",
                        "TARGET_TABLE_NAME",
                        "TARGET_COLUMN_NAME",
                        "TARGET_COLUMN_TYPE" )
                .addRow( "test", "Book", "id", "PrimaryKey", "test", "Book", "id", "PrimaryKey" )
                .addRow( "test", "Book", "author_first_name", "ForeignKey", "test", "Author", "first_name",
                        "PrimaryKey" )
                .addRow( "test", "Book", "author_last_name", "ForeignKey", "test", "Author", "last_name", "PrimaryKey" )
                .build();

        TableName book = new TableName( "test.Book" );
        TableName author = new TableName( "test.Author" );

        when( databaseClient.tables( Schema.UNDEFINED ) ).thenReturn( asList( book, author ) );
        when( databaseClient.executeQuery( any( String.class ) ) )
                .thenReturn( AwaitHandle.forReturnValue( bookSchema ) )
                .thenReturn( AwaitHandle.forReturnValue( bookResults ) )
                .thenReturn( AwaitHandle.forReturnValue( joinResults ) )
                .thenReturn( AwaitHandle.forReturnValue( authorTableSchema ) )
                .thenReturn( AwaitHandle.forReturnValue( addressResults ) );

        // when
        DatabaseInspector databaseInspector = new DatabaseInspector( databaseClient );
        SchemaExport schemaExport = databaseInspector.buildSchemaExport( Schema.UNDEFINED );

        // then
        List<TableName> tableNames = schemaExport.tables().stream().map( Table::name ).collect( Collectors.toList() );
        Join join = schemaExport.joins().stream().findFirst().get();

        assertThat( tableNames, matchesCollection( asList( book, author ) ) );
        assertThat( join.tableNames(), hasItems( book, author ) );
        assertEquals( asList( "test.Book.author_first_name\0test.Book.author_last_name",
                        "test.Author.first_name\0test.Author.last_name",
                        "test.Book.id",
                        "test.Book.id" ),
                keyNames( join ) );
        assertTrue( schemaExport.joinTables().isEmpty() );
    }

    @Test
    public void shouldExportTablesAndJoinsForSelfJoin() throws Exception
    {
        // given
        QueryResults employeeSchema = StubQueryResults.builder()
                .columns( "COLUMN_NAME", "REFERENCED_TABLE_NAME", "COLUMN_KEY" )
                .addRow( "id", null, "PRI" )
                .addRow( "name", null, "Data" )
                .addRow( "managerId", "Employee", "MUL" )
                .build();

        QueryResults employeeResults = StubQueryResults.builder()
                .columns( "COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE" )
                .addRow( "id", "INT", "PrimaryKey" )
                .addRow( "name", "TEXT", "Data" )
                .addRow( "managerId", "INT", "ForeignKey" )
                .build();

        QueryResults joinResults = StubQueryResults.builder()
                .columns( "SOURCE_TABLE_SCHEMA",
                        "SOURCE_TABLE_NAME",
                        "SOURCE_COLUMN_NAME",
                        "SOURCE_COLUMN_TYPE",
                        "TARGET_TABLE_SCHEMA",
                        "TARGET_TABLE_NAME",
                        "TARGET_COLUMN_NAME",
                        "TARGET_COLUMN_TYPE" )
                .addRow( "test", "Employee", "id", "PrimaryKey", "test", "Employee", "id", "PrimaryKey" )
                .addRow( "test", "Employee", "managerId", "ForeignKey", "test", "Employee", "id", "PrimaryKey" )
                .build();

        TableName employee = new TableName( "test.Employee" );

        when( databaseClient.tables( Schema.UNDEFINED ) ).thenReturn( singletonList( employee ) );
        when( databaseClient.executeQuery( any( String.class ) ) )
                .thenReturn( AwaitHandle.forReturnValue( employeeSchema ) )
                .thenReturn( AwaitHandle.forReturnValue( employeeResults ) )
                .thenReturn( AwaitHandle.forReturnValue( joinResults ) );

        // when
        DatabaseInspector databaseInspector = new DatabaseInspector( databaseClient );
        SchemaExport schemaExport = databaseInspector.buildSchemaExport( Schema.UNDEFINED );

        // then
        List<TableName> tableNames = schemaExport.tables().stream().map( Table::name ).collect( Collectors.toList() );
        Join join = schemaExport.joins().stream().findFirst().get();

        assertThat( tableNames, matchesCollection( singletonList( employee ) ) );
        assertThat( join.tableNames(), hasItems( employee ) );
        assertEquals( asList(
                "test.Employee.managerId", "test.Employee.id", "test.Employee.id", "test.Employee.id" ),
                keyNames( join ) );
        assertTrue( schemaExport.joinTables().isEmpty() );
    }

    @Test
    public void shouldExportTablesAndJoinTableForThreeTableJoin() throws Exception
    {
        // given
        QueryResults studentTableSchema = StubQueryResults.builder()
                .columns( "COLUMN_NAME", "REFERENCED_TABLE_NAME", "COLUMN_KEY" )
                .addRow( "id", null, "PRI" )
                .addRow( "username", null, "Data" )
                .build();

        QueryResults studentResults = StubQueryResults.builder()
                .columns( "COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE" )
                .addRow( "id", "INT", "PrimaryKey" )
                .addRow( "username", "TEXT", "Data" )
                .build();

        QueryResults courseTableSchema = StubQueryResults.builder()
                .columns( "COLUMN_NAME", "REFERENCED_TABLE_NAME", "COLUMN_KEY" )
                .addRow( "id", null, "PRI" )
                .addRow( "name", null, "Data" )
                .build();

        QueryResults courseResults = StubQueryResults.builder()
                .columns( "COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE" )
                .addRow( "id", "INT", "PrimaryKey" )
                .addRow( "name", "TEXT", "Data" )
                .build();

        QueryResults joinTableSchema = StubQueryResults.builder()
                .columns( "COLUMN_NAME", "REFERENCED_TABLE_NAME", "COLUMN_KEY" )
                .addRow( "studentId", "Student", "MUL" )
                .addRow( "courseId", "Course", "MUL" )
                .build();

        QueryResults joinTableResults = StubQueryResults.builder()
                .columns( "COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE" )
                .addRow( "studentId", "INT", "ForeignKey" )
                .addRow( "courseId", "INT", "ForeignKey" )
                .build();

        QueryResults joinResults = StubQueryResults.builder()
                .columns( "SOURCE_TABLE_SCHEMA",
                        "SOURCE_TABLE_NAME",
                        "SOURCE_COLUMN_NAME",
                        "SOURCE_COLUMN_TYPE",
                        "TARGET_TABLE_SCHEMA",
                        "TARGET_TABLE_NAME",
                        "TARGET_COLUMN_NAME",
                        "TARGET_COLUMN_TYPE" )
                .addRow( "test", "Student_Course", "studentId", "ForeignKey", "test", "Student", "id", "PrimaryKey" )
                .addRow( "test", "Student_Course", "courseId", "ForeignKey", "test", "Course", "id", "PrimaryKey" )
                .build();

        TableName student = new TableName( "test.Student" );
        TableName course = new TableName( "test.Course" );
        TableName studentCourse = new TableName( "test.Student_Course" );
        when( databaseClient.tables( Schema.UNDEFINED ) ).thenReturn( asList( student, course, studentCourse ) );
        when( databaseClient.executeQuery( any( String.class ) ) )
                .thenReturn( AwaitHandle.forReturnValue( studentTableSchema ) )
                .thenReturn( AwaitHandle.forReturnValue( studentResults ) )
                .thenReturn( AwaitHandle.forReturnValue( courseTableSchema ) )
                .thenReturn( AwaitHandle.forReturnValue( courseResults ) )
                .thenReturn( AwaitHandle.forReturnValue( joinTableSchema ) )
                .thenReturn( AwaitHandle.forReturnValue( joinResults ) )
                .thenReturn( AwaitHandle.forReturnValue( joinTableResults ) );

        // when
        DatabaseInspector databaseInspector = new DatabaseInspector( databaseClient );
        SchemaExport schemaExport = databaseInspector.buildSchemaExport( Schema.UNDEFINED );

        // then
        List<TableName> tableNames = schemaExport.tables().stream().map( Table::name ).collect( Collectors.toList() );
        JoinTable joinTable = new ArrayList<>( schemaExport.joinTables() ).get( 0 );

        assertThat( tableNames, matchesCollection( asList( student, course ) ) );
        assertThat( joinTable.join().tableNames(), matchesCollection( asList( student, studentCourse ) ) );
        assertEquals( asList( "test.Student_Course.studentId",
                        "test.Student.id",
                        "test.Student_Course.courseId",
                        "test.Course.id" ),
                keyNames( joinTable.join() ) );
        assertTrue( schemaExport.joins().isEmpty() );
    }

    private <T> TypeSafeMatcher<Collection<T>> matchesCollection( final Collection<T> expected )
    {
        return new TypeSafeMatcher<Collection<T>>()
        {
            @Override
            protected boolean matchesSafely( Collection<T> tables )
            {
                return tables.containsAll( expected ) && expected.containsAll( tables );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( expected.toString() );
            }
        };
    }

    private Collection<String> keyNames( Join join )
    {
        Collection<String> results = new ArrayList<>();
        results.add( join.keyTwoSourceColumn().name() );
        results.add( join.keyTwoTargetColumn().name() );
        results.add( join.keyOneSourceColumn().name() );
        results.add( join.keyOneTargetColumn().name() );
        return results;
    }
}
