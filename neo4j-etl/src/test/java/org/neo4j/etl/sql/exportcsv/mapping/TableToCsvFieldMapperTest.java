package org.neo4j.etl.sql.exportcsv.mapping;

import java.util.Collection;
import java.util.Collections;

import org.junit.Test;

import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.neo4j.importcsv.fields.CsvField;
import org.neo4j.etl.neo4j.importcsv.fields.IdSpace;
import org.neo4j.etl.neo4j.importcsv.fields.Neo4jDataType;
import org.neo4j.etl.sql.exportcsv.ColumnUtil;
import org.neo4j.etl.sql.exportcsv.io.TinyIntResolver;
import org.neo4j.etl.sql.metadata.ColumnRole;
import org.neo4j.etl.sql.metadata.ColumnValueSelectionStrategy;
import org.neo4j.etl.sql.metadata.CompositeColumn;
import org.neo4j.etl.sql.metadata.SimpleColumn;
import org.neo4j.etl.sql.metadata.SqlDataType;
import org.neo4j.etl.sql.metadata.Table;
import org.neo4j.etl.sql.metadata.TableName;

import static java.util.Arrays.asList;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

public class TableToCsvFieldMapperTest
{

    private final ColumnUtil columnUtil = new ColumnUtil();
    private TinyIntResolver tinyIntResolver = new TinyIntResolver(TinyIntAs.BOOLEAN );

    @Test
    public void shouldCreatePrimaryKeyAndDataMappingsForTable()
    {
        // given
        TableName personTable = new TableName( "test.Person" );

        Table table = Table.builder()
                .name( personTable )
                .addColumn( new CompositeColumn(
                        personTable,
                        Collections.singletonList( columnUtil.column( personTable, "id", ColumnRole.Data ) ),
                        ColumnRole.PrimaryKey ) )
                .addColumn( columnUtil.column( personTable, "username", ColumnRole.Data ) )
                .addColumn( new SimpleColumn( personTable, "age", ColumnRole.Data, SqlDataType.INT,
                        ColumnValueSelectionStrategy.SelectColumnValue,
                        Formatting.DEFAULT ) )
                .build();

        TableToCsvFieldMapper mapper = new TableToCsvFieldMapper( Formatting.DEFAULT, tinyIntResolver );

        // when
        ColumnToCsvFieldMappings mappings = mapper.createMappings( table );

        // then
        Collection<CsvField> fields = mappings.fields();

        assertThat( fields, contains(
                CsvField.id( new IdSpace( "test.Person" ) ),
                CsvField.data( "id", Neo4jDataType.String ),
                CsvField.data( "username", Neo4jDataType.String ),
                CsvField.data( "age", Neo4jDataType.Int ),
                CsvField.label() ) );
    }

    @Test
    public void shouldCreateCompositeKeyMappingsForTable()
    {
        // given
        TableName authorTable = new TableName( "test.Author" );

        Table table = Table.builder()
                .name( authorTable )
                .addColumn( new ColumnUtil().compositeKeyColumn( authorTable, asList( "first_name", "last_name" ),
                        ColumnRole.PrimaryKey ) )
                .build();

        TableToCsvFieldMapper mapper = new TableToCsvFieldMapper( Formatting.DEFAULT, tinyIntResolver );

        // when
        ColumnToCsvFieldMappings mappings = mapper.createMappings( table );

        // then
        Collection<CsvField> fields = mappings.fields();

        assertThat( fields, contains(
                CsvField.id( new IdSpace( "test.Author" ) ),
                CsvField.data( "first_name", Neo4jDataType.String ),
                CsvField.data( "last_name", Neo4jDataType.String ),
                CsvField.label() ) );
    }

    @Test
    public void shouldNotCreateMappingForForeignKey()
    {
        // given
        TableName personTable = new TableName( "test.Person" );

        Table table = Table.builder()
                .name( personTable )
                .addColumn( new CompositeColumn(
                        personTable,
                        Collections.singletonList( columnUtil.column( personTable, "id", ColumnRole.Data ) ),
                        ColumnRole.PrimaryKey ) )
                .addColumn( columnUtil.column( personTable, "username", ColumnRole.Data ) )
                .addColumn( new CompositeColumn(
                        personTable,
                        Collections.singletonList( columnUtil.column( personTable, "addressId", ColumnRole.Data ) ),
                        ColumnRole.ForeignKey ) )
                .build();

        TableToCsvFieldMapper mapper = new TableToCsvFieldMapper( Formatting.DEFAULT, tinyIntResolver );

        // when
        ColumnToCsvFieldMappings mappings = mapper.createMappings( table );

        // then
        Collection<CsvField> fields = mappings.fields();

        assertThat( fields, contains(
                CsvField.id( new IdSpace( "test.Person" ) ),
                CsvField.data( "id", Neo4jDataType.String ),
                CsvField.data( "username", Neo4jDataType.String ),
                CsvField.label() ) );
    }
}
