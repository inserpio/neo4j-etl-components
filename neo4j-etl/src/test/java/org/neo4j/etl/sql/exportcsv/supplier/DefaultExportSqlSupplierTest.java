package org.neo4j.etl.sql.exportcsv.supplier;

import java.util.Arrays;

import org.junit.Test;

import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.neo4j.importcsv.fields.CsvField;
import org.neo4j.etl.neo4j.importcsv.fields.IdSpace;
import org.neo4j.etl.neo4j.importcsv.fields.Neo4jDataType;
import org.neo4j.etl.sql.exportcsv.ColumnUtil;
import org.neo4j.etl.sql.exportcsv.mapping.ColumnToCsvFieldMapping;
import org.neo4j.etl.sql.exportcsv.mapping.ColumnToCsvFieldMappings;
import org.neo4j.etl.sql.metadata.Column;
import org.neo4j.etl.sql.metadata.ColumnRole;
import org.neo4j.etl.sql.metadata.CompositeColumn;
import org.neo4j.etl.sql.metadata.TableName;

import static org.junit.Assert.assertEquals;

public class DefaultExportSqlSupplierTest
{
    private ColumnUtil columnUtil = new ColumnUtil();

    @Test
    public void shouldCreateSqlForSelectingColumnsFromTables()
    {
        // given
        TableName table = new TableName( "test.Person" );
        Column column1 = columnUtil.column( table, "id", ColumnRole.PrimaryKey );

        Column column2 = columnUtil.column( table, "username", ColumnRole.Data );

        Column column3 = columnUtil.column( table, "age", ColumnRole.Data );

        final CsvField id = CsvField.id( new IdSpace( table.fullName() ) );
        ColumnToCsvFieldMappings mappings = ColumnToCsvFieldMappings.builder()
                .add( new ColumnToCsvFieldMapping( column1, id ) )
                .add( new ColumnToCsvFieldMapping( column2, CsvField.data( "username", Neo4jDataType.String ) ) )
                .add( new ColumnToCsvFieldMapping( column3, CsvField.data( "age", Neo4jDataType.String ) ) )
                .withFormatting(Formatting.DEFAULT )
                .build();

        DefaultExportSqlSupplier sqlSupplier = new DefaultExportSqlSupplier();

        // when
        String sql = sqlSupplier.sql( mappings );

        // then
        String expectedSql = "SELECT " +
                "`test`.`Person`.`id` AS `id`, " +
                "`test`.`Person`.`username` AS `username`, " +
                "`test`.`Person`.`age` AS `age` " +
                "FROM `test`.`Person`";

        assertEquals( expectedSql, sql );
    }

    @Test
    public void shouldCreateSqlForCompositeColumnsFromTables()
    {
        // given
        TableName forTable = new TableName( "test.Author" );
        Column firstName = columnUtil.column( forTable, "first_name", ColumnRole.PrimaryKey );

        Column lastName = columnUtil.column( forTable, "last_name", ColumnRole.PrimaryKey );

        CompositeColumn from = new CompositeColumn(
                forTable, Arrays.asList( firstName, lastName ), ColumnRole.PrimaryKey );
        ColumnToCsvFieldMappings mappings = ColumnToCsvFieldMappings.builder()
                .add( new ColumnToCsvFieldMapping( from, CsvField.id() ) )
                .withFormatting(Formatting.DEFAULT )
                .build();

        DefaultExportSqlSupplier sqlSupplier = new DefaultExportSqlSupplier();

        // when
        String sql = sqlSupplier.sql( mappings );

        // then
        String expectedSql = "SELECT `test`.`Author`.`first_name` AS `first_name`, " +
                "`test`.`Author`.`last_name` AS `last_name` " +
                "FROM `test`.`Author`";

        assertEquals( expectedSql, sql );
    }
}
