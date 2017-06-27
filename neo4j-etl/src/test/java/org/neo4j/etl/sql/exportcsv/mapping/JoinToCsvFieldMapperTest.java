package org.neo4j.etl.sql.exportcsv.mapping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

import org.junit.Test;

import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.neo4j.importcsv.fields.CsvField;
import org.neo4j.etl.neo4j.importcsv.fields.IdSpace;
import org.neo4j.etl.sql.exportcsv.ColumnUtil;
import org.neo4j.etl.sql.metadata.Column;
import org.neo4j.etl.sql.metadata.ColumnRole;
import org.neo4j.etl.sql.metadata.Join;
import org.neo4j.etl.sql.metadata.JoinKey;
import org.neo4j.etl.sql.metadata.TableName;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;

public class JoinToCsvFieldMapperTest
{

    private JoinToCsvFieldMapper mapper = new JoinToCsvFieldMapper( Formatting.DEFAULT,
            new RelationshipNameResolver( RelationshipNameFrom.TABLE_NAME ) );
    private ColumnUtil columnUtil = new ColumnUtil();

    @Test
    public void shouldCreateMappingsForJoinTable()
    {
        // given
        TableName leftTable = new TableName( "test.Person" );
        TableName rightTable = new TableName( "test.Address" );
        Join join = new Join(
                new JoinKey(
                        columnUtil.column( leftTable, "id", ColumnRole.PrimaryKey ),
                        columnUtil.column( leftTable, "id", ColumnRole.PrimaryKey ) ),
                new JoinKey(
                        columnUtil.column( leftTable, "addressId", ColumnRole.ForeignKey ),
                        columnUtil.column( rightTable, "id", ColumnRole.PrimaryKey ) )
        );
        // when
        ColumnToCsvFieldMappings mappings = mapper.createMappings( join );

        // then
        Collection<CsvField> fields = new ArrayList<>( mappings.fields() );
        Collection<String> columns = mappings.columns().stream().map( Column::name ).collect( Collectors.toList() );

        assertEquals( fields, asList(
                CsvField.startId( new IdSpace( "test.Person" ) ),
                CsvField.endId( new IdSpace( "test.Address" ) ),
                CsvField.relationshipType() ) );

        assertEquals( asList( "test.Person.id", "test.Person.addressId", "ADDRESS" ), columns );
    }

    @Test
    public void shouldCreateMappingsForJoinTableUsingColumnNameAsRelationshipName()
    {
        // given
        TableName leftTable = new TableName( "test.Person" );
        TableName rightTable = new TableName( "test.Address" );
        Join join = new Join(
                new JoinKey(
                        columnUtil.column( leftTable, "id", ColumnRole.PrimaryKey ),
                        columnUtil.column( leftTable, "id", ColumnRole.PrimaryKey ) ),
                new JoinKey(
                        columnUtil.column( leftTable, "addressId", ColumnRole.ForeignKey ),
                        columnUtil.column( rightTable, "id", ColumnRole.PrimaryKey ) )
        );
        // when
        ColumnToCsvFieldMappings mappings = new JoinToCsvFieldMapper( Formatting.DEFAULT,
                new RelationshipNameResolver( RelationshipNameFrom.COLUMN_NAME ) ).createMappings( join );

        // then
        Collection<CsvField> fields = new ArrayList<>( mappings.fields() );
        Collection<String> columns = mappings.columns().stream().map( Column::name ).collect( Collectors.toList() );

        assertEquals( fields, asList(
                CsvField.startId( new IdSpace( "test.Person" ) ),
                CsvField.endId( new IdSpace( "test.Address" ) ),
                CsvField.relationshipType() ) );

        assertEquals( asList( "test.Person.id", "test.Person.addressId", "ADDRESS_ID" ), columns );
    }

    @Test
    public void shouldCreateMappingsForJoinTableUsingColumnNameAsRelationshipNameForCompositeColumns()
    {
        // given
        TableName leftTable = new TableName( "test.Book" );
        TableName authorTable = new TableName( "test.Author" );

        Join join = new Join(
                new JoinKey(
                        columnUtil.column( leftTable, "id", ColumnRole.PrimaryKey ),
                        columnUtil.column( leftTable, "id", ColumnRole.PrimaryKey ) ),
                new JoinKey(
                        columnUtil.compositeKeyColumn( leftTable,
                                asList( "author_first_name", "author_last_name" ),
                                ColumnRole.ForeignKey ),
                        new ColumnUtil().compositeKeyColumn( authorTable, asList( "first_name", "last_name" ),
                                ColumnRole.PrimaryKey ) )
        );
        // when
        ColumnToCsvFieldMappings mappings = new JoinToCsvFieldMapper( Formatting.DEFAULT,
                new RelationshipNameResolver( RelationshipNameFrom.COLUMN_NAME ) ).createMappings( join );

        // then
        Collection<CsvField> fields = new ArrayList<>( mappings.fields() );
        Collection<String> columns = mappings.columns().stream().map( Column::name ).collect( Collectors.toList() );

        assertEquals( fields, asList(
                CsvField.startId( new IdSpace( "test.Book" ) ),
                CsvField.endId( new IdSpace( "test.Author" ) ),
                CsvField.relationshipType() ) );

        assertEquals( asList( "test.Book.id",
                "test.Book.author_first_name\0test.Book.author_last_name",
                "AUTHOR_FIRST_NAME_AUTHOR_LAST_NAME" ), columns );
    }
}
