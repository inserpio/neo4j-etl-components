package org.neo4j.integration.sql.metadata;

import org.neo4j.integration.util.Preconditions;

public class Column
{
    public static Column.Builder.SetTable builder()
    {
        return new ColumnBuilder();
    }

    private final TableName table;
    private final String name;
    private final ColumnType type;

    Column( ColumnBuilder builder )
    {
        this.table = Preconditions.requireNonNull( builder.table, "Table" );
        this.name = Preconditions.requireNonNullString( builder.name, "Name" );
        this.type = Preconditions.requireNonNull( builder.type, "Type" );
    }

    public TableName table()
    {
        return table;
    }

    public String fullName()
    {
        return table.fullyQualifiedColumnName( name );
    }

    public String simpleName()
    {
        return name;
    }

    public String name()
    {
        return type.name( this );
    }

    public ColumnType type()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return "Column{" +
                "table=" + table +
                ", name='" + name + '\'' +
                ", type=" + type +
                '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        Column column = (Column) o;

        return table.equals( column.table ) && name.equals( column.name ) && type == column.type;
    }

    @Override
    public int hashCode()
    {
        int result = table.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    public interface Builder
    {
        interface SetTable
        {
            SetName table( TableName table );
        }

        interface SetName
        {
            SetType name( String name );
        }

        interface SetType
        {
            Builder type( ColumnType type );
        }

        Column build();
    }
}
