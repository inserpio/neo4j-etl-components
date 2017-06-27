package org.neo4j.etl.sql.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import org.neo4j.etl.sql.RowAccessor;
import org.neo4j.etl.sql.exportcsv.io.TinyIntResolver;
import org.neo4j.etl.sql.exportcsv.mapping.ColumnToCsvFieldMappings;

import static java.lang.String.format;

public class CompositeColumn implements Column
{
    public static Column fromJson( JsonNode root )
    {
        TableName table = new TableName( root.path( "table" ).textValue() );
        ColumnRole role = ColumnRole.valueOf( root.path( "role" ).textValue() );

        Collection<Column> columns = new ArrayList<>();
        ArrayNode columnArray = (ArrayNode) root.path( "columns" );
        for ( JsonNode jsonNode : columnArray )
        {
            columns.add( Column.fromJson( jsonNode ) );
        }

        return new CompositeColumn( table, columns, role );
    }

    public static final String SEPARATOR = "\0";

    private final TableName table;
    private final Collection<Column> columns;
    private final ColumnRole role;

    public CompositeColumn( TableName table, Collection<Column> columns, ColumnRole role )
    {
        this.table = table;
        this.columns = columns;
        this.role = role;

        for ( Column column : columns )
        {
            if ( !column.table().equals( table ) )
            {
                throw new IllegalArgumentException(
                        format( "Column '%s' is not associated with table '%s", column.name(), table.fullName() ) );
            }
        }
    }

    public Collection<Column> columns()
    {
        return columns;
    }

    @Override
    public TableName table()
    {
        return this.table;
    }

    @Override
    public String name()
    {
        return columns.stream().map( Column::name ).collect( Collectors.joining( SEPARATOR ) );
    }

    @Override
    public String alias()
    {
        return columns.stream().map( Column::alias ).collect( Collectors.joining( SEPARATOR ) );
    }

    @Override
    public ColumnRole role()
    {
        return role;
    }

    @Override
    public SqlDataType sqlDataType()
    {
        return SqlDataType.TEXT;
    }

    @Override
    public boolean allowAddToSelectStatement()
    {
        return columns.stream().allMatch( Column::allowAddToSelectStatement );
    }

    @Override
    public String selectFrom( RowAccessor row, int rowIndex )
    {
        List<String> values = columns.stream()
                .map( c -> row.getString( c.alias() ) )
                .collect( Collectors.toList() );

        if ( values.stream().anyMatch( StringUtils::isEmpty ) )
        {
            return StringUtils.EMPTY;
        }
        else
        {
            return values.stream()
                    .filter( StringUtils::isNotEmpty )
                    .collect( Collectors.joining( SEPARATOR ) );
        }
    }

    @Override
    public String aliasedColumn()
    {
        return columns.stream().map( Column::aliasedColumn ).collect( Collectors.joining( ", " ) );
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals( Object o )
    {
        return EqualsBuilder.reflectionEquals( this, o );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString( this );
    }

    @Override
    public void addData( ColumnToCsvFieldMappings.Builder builder, TinyIntResolver tinyIntResolver )
    {
        columns.forEach( column -> column.addData( builder, tinyIntResolver ) );
    }

    @Override
    public JsonNode toJson()
    {
        ObjectNode root = JsonNodeFactory.instance.objectNode();

        root.put( "type", getClass().getSimpleName() );
        root.put( "table", table.fullName() );
        root.put( "role", role.name() );

        ArrayNode array = JsonNodeFactory.instance.arrayNode();

        for ( Column column : columns )
        {
            array.add( column.toJson() );
        }

        root.set( "columns", array );

        return root;
    }

    @Override
    public boolean useQuotes()
    {
        return true;
    }
}
