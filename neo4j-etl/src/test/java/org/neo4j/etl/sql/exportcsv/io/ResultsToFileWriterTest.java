package org.neo4j.etl.sql.exportcsv.io;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.etl.neo4j.importcsv.config.GraphObjectType;
import org.neo4j.etl.neo4j.importcsv.config.formatting.Delimiter;
import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.sql.QueryResults;
import org.neo4j.etl.sql.StubQueryResults;
import org.neo4j.etl.sql.exportcsv.ColumnUtil;
import org.neo4j.etl.sql.exportcsv.mapping.ColumnToCsvFieldMappings;
import org.neo4j.etl.sql.exportcsv.mapping.MetadataMapping;
import org.neo4j.etl.sql.exportcsv.mapping.TinyIntAs;
import org.neo4j.etl.sql.metadata.ColumnRole;
import org.neo4j.etl.sql.metadata.ColumnValueSelectionStrategy;
import org.neo4j.etl.sql.metadata.SimpleColumn;
import org.neo4j.etl.sql.metadata.SqlDataType;
import org.neo4j.etl.sql.metadata.TableName;
import org.neo4j.etl.util.ResourceRule;
import org.neo4j.etl.util.TemporaryDirectory;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResultsToFileWriterTest
{
    @Rule
    public final ResourceRule<Path> tempDirectory = new ResourceRule<>( TemporaryDirectory.temporaryDirectory() );
    private ColumnUtil columnUtil = new ColumnUtil();
    private TableName table = new TableName( "users" );
    private final ColumnToCsvFieldMappings mappings = mock( ColumnToCsvFieldMappings.class );
    private Path exportFile = tempDirectory.get().resolve( "export-file.csv" );
    private static final Formatting TAB_DELIMITER = Formatting.builder().delimiter( Delimiter.TAB ).build();
    private final ResultsToFileWriter resultsToFileWriter = new ResultsToFileWriter( TAB_DELIMITER,
            new TinyIntResolver( TinyIntAs.BYTE ) );

    @Test
    public void shouldWriteCsvFile() throws Exception
    {
        // given
        QueryResults results = StubQueryResults.builder()
                .columns( "id", "username" )
                .addRow( "1", "user-1" )
                .addRow( "2", "user-2" )
                .build();

        when( mappings.columns() ).thenReturn(
                asList(
                        columnUtil.keyColumn( table, "id", ColumnRole.PrimaryKey ),
                        columnUtil.column( table, "username", ColumnRole.Data ) ) );

        MetadataMapping resource = new MetadataMapping( table.fullName(), GraphObjectType.Node, "SELECT ...",
                mappings );

        // when
        resultsToFileWriter.write( results, exportFile, resource );

        // then
        List<String> contents = Files.readAllLines( exportFile );
        assertEquals( asList( "\"1\"\t\"user-1\"", "\"2\"\t\"user-2\"" ), contents );
    }

    @Test
    public void shouldWriteCompositeColumns() throws Exception
    {
        // given
        QueryResults results = StubQueryResults.builder()
                .columns( "first-name", "last-name" )
                .addRow( "Jane", "Smith" )
                .addRow( "John", "Smith" )
                .build();

        when( mappings.columns() ).thenReturn(
                Collections.singletonList(
                        columnUtil.compositeKeyColumn(
                                table, asList( "first-name", "last-name" ), ColumnRole.PrimaryKey ) ) );

        MetadataMapping resource = new MetadataMapping( table.fullName(), GraphObjectType.Node, "SELECT ...",
                mappings );

        // when
        resultsToFileWriter.write( results, exportFile, resource );

        // then
        List<String> contents = Files.readAllLines( exportFile );
        assertEquals( asList( "\"Jane\0Smith\"", "\"John\0Smith\"" ), contents );
    }

    @Test
    public void shouldNotAddQuotationForNonStringValues() throws Exception
    {
        // given
        QueryResults results = StubQueryResults.builder()
                .columns( "id", "username" )
                .addRow( "1", "user-1" )
                .addRow( "2", "user-2" )
                .build();

        when( mappings.columns() ).thenReturn(
                asList(
                        new SimpleColumn( table, "id", ColumnRole.Data, SqlDataType.INT,
                                ColumnValueSelectionStrategy.SelectColumnValue, Formatting.DEFAULT ),
                        new SimpleColumn( table, "username", ColumnRole.Data, SqlDataType.VARCHAR,
                                ColumnValueSelectionStrategy.SelectColumnValue, Formatting.DEFAULT ) ) );

        MetadataMapping resource = new MetadataMapping( table.fullName(), GraphObjectType.Node, "SELECT ...",
                mappings );

        // when
        resultsToFileWriter.write( results, exportFile, resource );

        // then
        List<String> contents = Files.readAllLines( exportFile );
        assertEquals( asList( "1\t\"user-1\"", "2\t\"user-2\"" ), contents );
    }

    @Test
    public void shouldWriteDelimiterForNullOrEmptyValues() throws Exception
    {
        // given

        QueryResults results = StubQueryResults.builder()
                .columns( "id", "username", "age" )
                .addRow( "1", "user-1", "45" )
                .addRow( "2", "", "32" )
                .addRow( "3", "user-3", null )
                .addRow( "4", "user-4", "" )
                .build();

        when( mappings.columns() ).thenReturn(
                asList(
                        columnUtil.keyColumn( table, "id", ColumnRole.PrimaryKey ),
                        columnUtil.column( table, "username", ColumnRole.Data ),
                        columnUtil.column( table, "age", ColumnRole.Data ) ) );

        MetadataMapping resource = new MetadataMapping( table.fullName(), GraphObjectType.Node, "SELECT ...",
                mappings );

        //when
        resultsToFileWriter.write( results, exportFile, resource );

        // then
        List<String> contents = Files.readAllLines( exportFile );
        assertEquals(
                asList( "\"1\"\t\"user-1\"\t\"45\"", "\"2\"\t\t\"32\"", "\"3\"\t\"user-3\"\t", "\"4\"\t\"user-4\"\t" ),
                contents );
    }

    @Test
    public void shouldSkipWritingRowForRelationshipsIfAnyColumnHasNullOrEmptyValues() throws Exception
    {
        // given
        QueryResults results = StubQueryResults.builder()
                .columns( "id", "username" )
                .addRow( "1", "user-1" )
                .addRow( "2", "" )
                .addRow( "3", null )
                .addRow( "", "user-4" )
                .build();

        when( mappings.columns() ).thenReturn(
                asList(
                        columnUtil.keyColumn( table, "id", ColumnRole.PrimaryKey ),
                        columnUtil.column( table, "username", ColumnRole.Data ) ) );

        MetadataMapping resource = new MetadataMapping(
                table.fullName(),
                GraphObjectType.Relationship,
                "SELECT ...",
                mappings );

        // when
        resultsToFileWriter.write( results, exportFile, resource );

        // then
        List<String> contents = Files.readAllLines( exportFile );
        assertEquals( asList( "\"1\"\t\"user-1\"", "\"2\"\t", "\"3\"\t" ), contents );
    }

    @Test
    public void shouldWriteTinyIntAsByte() throws Exception
    {
        // given
        QueryResults results = StubQueryResults.builder()
                .columns( "id", "tiny_int_value" )
                .addRow( "1", "1" )
                .build();

        when( mappings.columns() ).thenReturn(
                asList(
                        columnUtil.keyColumn( table, "id", ColumnRole.PrimaryKey ),
                        new SimpleColumn( table, "tiny_int_value", ColumnRole.Data, SqlDataType.TINYINT,
                                ColumnValueSelectionStrategy.SelectColumnValue, Formatting.DEFAULT ) ) );

        MetadataMapping resource = new MetadataMapping(
                table.fullName(),
                GraphObjectType.Node,
                "SELECT ...",
                mappings );

        // when
        resultsToFileWriter.write( results, exportFile, resource );

        // then
        List<String> contents = Files.readAllLines( exportFile );
        assertEquals( asList( "\"1\"\t1" ), contents );
    }

    @Test
    public void shouldWriteTinyIntAsBoolean() throws Exception
    {
        // given
        QueryResults results = StubQueryResults.builder()
                .columns( "id", "tiny_int_value" )
                .addRow( "1", "1" )
                .build();

        when( mappings.columns() ).thenReturn(
                asList(
                        columnUtil.keyColumn( table, "id", ColumnRole.PrimaryKey ),
                        new SimpleColumn( table, "tiny_int_value", ColumnRole.Data, SqlDataType.TINYINT,
                                ColumnValueSelectionStrategy.SelectColumnValue, Formatting.DEFAULT ) ) );

        MetadataMapping resource = new MetadataMapping(
                table.fullName(),
                GraphObjectType.Node,
                "SELECT ...",
                mappings );

        // when
        ResultsToFileWriter resultsToFileWriter = new ResultsToFileWriter( TAB_DELIMITER,
                new TinyIntResolver( TinyIntAs.BOOLEAN ) );


        resultsToFileWriter.write( results, exportFile, resource );

        // then
        List<String> contents = Files.readAllLines( exportFile );
        assertEquals( asList( "\"1\"\ttrue" ), contents );
    }
}
