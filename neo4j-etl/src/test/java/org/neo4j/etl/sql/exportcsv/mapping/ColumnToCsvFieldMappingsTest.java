package org.neo4j.etl.sql.exportcsv.mapping;

import java.util.Collection;

import org.junit.Test;

import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.neo4j.importcsv.fields.CsvField;
import org.neo4j.etl.neo4j.importcsv.fields.Neo4jDataType;
import org.neo4j.etl.sql.exportcsv.ColumnUtil;
import org.neo4j.etl.sql.metadata.Column;
import org.neo4j.etl.sql.metadata.ColumnRole;
import org.neo4j.etl.sql.metadata.TableName;

import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;

public class ColumnToCsvFieldMappingsTest
{
    private ColumnUtil columnUtil = new ColumnUtil();

    @Test
    public void shouldReturnCollectionOfCsvFields()
    {
        // given
        TableName personTable = new TableName( "test.Person" );

        Column column1 = columnUtil.keyColumn( personTable, "id", ColumnRole.PrimaryKey );

        Column column2 = columnUtil.column( personTable, "test.Person.username", "username", ColumnRole.Data );

        Column column3 = columnUtil.column( personTable, "test.Person.age", "age", ColumnRole.Data );

        CsvField idField = CsvField.id();
        CsvField usernameField = CsvField.data( "username", Neo4jDataType.String );
        CsvField ageField = CsvField.data( "age", Neo4jDataType.String );

        ColumnToCsvFieldMappings mappings = ColumnToCsvFieldMappings.builder()
                .add( new ColumnToCsvFieldMapping( column1, idField ) )
                .add( new ColumnToCsvFieldMapping( column2, usernameField ) )
                .add( new ColumnToCsvFieldMapping( column3, ageField ) )
                .withFormatting(Formatting.DEFAULT )
                .build();

        // when
        Collection<CsvField> fields = mappings.fields();

        // then
        assertThat( fields, hasItems( idField, usernameField, ageField ) );
    }

    @Test
    public void shouldReturnCollectionOfFullyQualifiedColumnNames()
    {
        // given
        TableName personTable = new TableName( "test.Person" );

        Column column1 = columnUtil.keyColumn( personTable, "id", ColumnRole.PrimaryKey );

        Column column2 = columnUtil.column( personTable, "test.Person.username", "username", ColumnRole.Data );

        Column column3 = columnUtil.column( personTable, "test.Person.age", "age", ColumnRole.Data );

        ColumnToCsvFieldMappings mappings = ColumnToCsvFieldMappings.builder()
                .add( new ColumnToCsvFieldMapping( column1, CsvField.id() ) )
                .add( new ColumnToCsvFieldMapping( column2, CsvField.data( "username", Neo4jDataType.String ) ) )
                .add( new ColumnToCsvFieldMapping( column3, CsvField.data( "age", Neo4jDataType.String ) ) )
                .withFormatting(Formatting.DEFAULT )
                .build();

        // when
        Collection<Column> columns = mappings.columns();

        // then
        assertThat( columns, hasItems( column1, column2, column3 ) );
    }

    @Test
    public void shouldReturnCollectionOfAliasedColumnNames()
    {
        // given
        TableName authorTable = new TableName( "test.Author" );
        Column column1 = columnUtil.compositeKeyColumn(
                authorTable,
                asList( "first_name", "last_name" ),
                ColumnRole.PrimaryKey );


        Column column2 = columnUtil.column( authorTable, "age", "age", ColumnRole.Data );

        ColumnToCsvFieldMappings mappings = ColumnToCsvFieldMappings.builder()
                .add( new ColumnToCsvFieldMapping( column1, CsvField.id() ) )
                .add( new ColumnToCsvFieldMapping( column2, CsvField.data( "age", Neo4jDataType.String ) ) )
                .withFormatting(Formatting.DEFAULT )
                .build();

        // when
        Collection<String> aliasedColumns = mappings.aliasedColumns();

        // then
        assertThat( aliasedColumns,
                hasItems(
                        "`test`.`Author`.`first_name` AS `first_name`, `test`.`Author`.`last_name` AS `last_name`",
                        "`test`.`Author`.`age` AS `age`" ) );
    }

    @Test
    public void shouldReturnCollectionOfFullyQualifiedTableNames()
    {
        // given
        TableName personTable = new TableName( "test.Person" );

        Column column1 = columnUtil.keyColumn( personTable, "id", ColumnRole.PrimaryKey );

        Column column2 = columnUtil.column( personTable, "username", "username", ColumnRole.Data );
        Column column3 = columnUtil.column(
                new TableName( "test.Address" ),
                "postcode",
                "postcode",
                ColumnRole.PrimaryKey );

        ColumnToCsvFieldMappings mappings = ColumnToCsvFieldMappings.builder()
                .add( new ColumnToCsvFieldMapping( column1, CsvField.id() ) )
                .add( new ColumnToCsvFieldMapping( column2, CsvField.data( "username", Neo4jDataType.String ) ) )
                .add( new ColumnToCsvFieldMapping( column3, CsvField.data( "id", Neo4jDataType.String ) ) )
                .withFormatting(Formatting.DEFAULT )
                .build();

        // when
        Collection<String> tableNames = mappings.tableNames();

        // then
        assertThat( tableNames, hasItems( "`test`.`Person`", "`test`.`Address`" ) );
    }
}
