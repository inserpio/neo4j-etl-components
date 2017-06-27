package org.neo4j.etl.sql.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;

import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.neo4j.importcsv.config.formatting.QuoteChar;
import org.neo4j.etl.sql.QueryResults;
import org.neo4j.etl.sql.RowAccessor;
import org.neo4j.etl.sql.StubQueryResults;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class SimpleColumnTest
{

    @Test
    public void shouldReturnCollectionOfAliasedColumnNames()
    {
        // given
        TableName personTable = new TableName( "test.Person" );
        Column column1 = new SimpleColumn(
                personTable,
                "id",
                "id-alias",
                ColumnRole.PrimaryKey,
                SqlDataType.INT,
                ColumnValueSelectionStrategy.SelectColumnValue,
                Formatting.DEFAULT );
        Column column2 = new SimpleColumn(
                personTable,
                "username",
                ColumnRole.Data,
                SqlDataType.TEXT,
                ColumnValueSelectionStrategy.SelectColumnValue,
                Formatting.DEFAULT );
        Column column3 = new SimpleColumn( personTable,
                "PERSON",
                "PERSON",
                ColumnRole.Literal,
                SqlDataType.TEXT,
                ColumnValueSelectionStrategy.SelectColumnValue,
                Formatting.DEFAULT );

        // then
        assertThat( column1.aliasedColumn(), is( "`test`.`Person`.`id` AS `id-alias`" ) );
        assertThat( column2.aliasedColumn(), is( "`test`.`Person`.`username` AS `username`" ) );
        assertThat( column3.aliasedColumn(), is( "\"PERSON\" AS `PERSON`" ) );
    }

    @Test
    public void selectRowShouldGetValueFromResults() throws Exception
    {
        // given
        TableName personTable = new TableName( "test.Person" );
        QueryResults results = StubQueryResults.builder()
                .columns( "id", "username" )
                .addRow( "1", "user-1" )
                .build();

        Column column1 = new SimpleColumn(
                personTable,
                "id",
                ColumnRole.Data,
                SqlDataType.INT,
                ColumnValueSelectionStrategy.SelectColumnValue,
                Formatting.DEFAULT );
        Column column2 = new SimpleColumn(
                personTable,
                "username",
                ColumnRole.Data,
                SqlDataType.TEXT,
                ColumnValueSelectionStrategy.SelectColumnValue,
                Formatting.DEFAULT );

        // then
        results.next();
        assertThat( column1.selectFrom( results, 1 ), is( "1" ) );
        assertThat( column2.selectFrom( results, 1 ), is( "user-1" ) );
    }

    @Test
    public void selectRowFromShouldNotAddQuotesForNullValues() throws Exception
    {
        // given
        TableName personTable = new TableName( "test.Person" );
        QueryResults results = StubQueryResults.builder()
                .columns( "id", "username" )
                .addRow( null, null )
                .build();

        Column column1 = new SimpleColumn(
                personTable,
                "id",
                ColumnRole.Data,
                SqlDataType.INT,
                ColumnValueSelectionStrategy.SelectColumnValue,
                Formatting.DEFAULT );
        Column column2 = new SimpleColumn(
                personTable,
                "username",
                ColumnRole.Data,
                SqlDataType.TEXT,
                ColumnValueSelectionStrategy.SelectColumnValue,
                Formatting.DEFAULT );

        // then
        results.next();
        assertNull( column1.selectFrom( results, 1 ) );
        assertNull( column2.selectFrom( results, 1 ) );
    }

    @Test
    public void shouldNotAddBackTicksForLiteralColumns()
    {
        // given
        TableName personTable = new TableName( "test.Person" );
        Column labelColumn = new SimpleColumn(
                personTable,
                // We don't need to escape with quote character because it's done automatically if literal
                "Person",
                "Person",
                ColumnRole.Literal,
                SqlDataType.TEXT,
                ColumnValueSelectionStrategy.SelectColumnValue,
                Formatting.DEFAULT );
        // then
        assertThat( labelColumn.aliasedColumn(), is( "\"Person\" AS `Person`" ) );
    }

    @Test
    public void shouldSerializeToAndDeserializeFromJson()
    {
        // given
        TableName personTable = new TableName( "test.Person" );
        Column column = new SimpleColumn(
                personTable,
                "id",
                "id-alias",
                ColumnRole.PrimaryKey,
                SqlDataType.INT,
                ColumnValueSelectionStrategy.SelectColumnValue,
                Formatting.DEFAULT );

        JsonNode json = column.toJson();

        // when
        Column deserialized = Column.fromJson( json );

        // then
        assertEquals( column, deserialized );
    }

    @Test
    public void shouldAllowBeingBeAddedToSelectStatementIfSelectingColumnValue()
    {
        // given
        TableName personTable = new TableName( "test.Person" );
        Column column = new SimpleColumn(
                personTable,
                "id",
                "id-alias",
                ColumnRole.PrimaryKey,
                SqlDataType.INT,
                ColumnValueSelectionStrategy.SelectColumnValue,
                Formatting.DEFAULT );

        // then
        assertTrue( column.allowAddToSelectStatement() );
    }

    @Test
    public void shouldSelectColumnValueIfUsingSelectColumnValueStrategy()
    {
        // given
        TableName personTable = new TableName( "test.Person" );
        Column column = new SimpleColumn(
                personTable,
                "id",
                "id-alias",
                ColumnRole.PrimaryKey,
                SqlDataType.INT,
                ColumnValueSelectionStrategy.SelectColumnValue,
                Formatting.DEFAULT );

        RowAccessor row = mock( RowAccessor.class );

        // when
        column.selectFrom( row, 10 );

        // then
        verify( row ).getString( "id-alias" );
    }

    @Test
    public void shouldNotAllowBeingBeAddedToSelectStatementIfSelectingRowIndex()
    {
        // given
        TableName personTable = new TableName( "test.Person" );
        Column column = new SimpleColumn(
                personTable,
                "id",
                "id-alias",
                ColumnRole.PrimaryKey,
                SqlDataType.INT,
                ColumnValueSelectionStrategy.SelectRowIndex,
                Formatting.DEFAULT );

        // then
        assertFalse( column.allowAddToSelectStatement() );
    }

    @Test
    public void shouldSelectRowIndexIfUsingSelectRowIndexStrategy()
    {
        // given
        TableName personTable = new TableName( "test.Person" );
        Column column = new SimpleColumn(
                personTable,
                "id",
                "id-alias",
                ColumnRole.PrimaryKey,
                SqlDataType.INT,
                ColumnValueSelectionStrategy.SelectRowIndex,
                Formatting.DEFAULT );

        RowAccessor row = mock( RowAccessor.class );

        // when
        String result = column.selectFrom( row, 10 );

        // then
        verifyZeroInteractions( row );
        assertEquals( "10", result );
    }
}
