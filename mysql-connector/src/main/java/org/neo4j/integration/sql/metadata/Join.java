package org.neo4j.integration.sql.metadata;

import java.util.Collection;

import org.neo4j.integration.util.Preconditions;

import static java.lang.String.format;
import static java.util.Arrays.asList;

public class Join implements DatabaseObject
{
    public static Builder.SetParentTable builder()
    {
        return new JoinBuilder();
    }

    private final Column primaryKey;
    private final Column foreignKey;
    private final TableName childTable;

    Join( JoinBuilder builder )
    {
        this.primaryKey = Preconditions.requireNonNull( builder.primaryKey, "Primary key" );
        this.foreignKey = Preconditions.requireNonNull( builder.foreignKey, "Foreign key" );
        this.childTable = Preconditions.requireNonNull( builder.childTable, "Child table" );
    }

    public Column primaryKey()
    {
        return primaryKey;
    }

    public Column foreignKey()
    {
        return foreignKey;
    }

    public TableName childTable()
    {
        return childTable;
    }

    public Collection<TableName> tableNames()
    {
        return asList( primaryKey.table(), childTable );
    }

    @Override
    public String descriptor()
    {
        return format("%s_%s", primaryKey.table().fullName(), childTable.fullName());
    }

    @Override
    public String toString()
    {
        return "Join{" +
                "primaryKey=" + primaryKey +
                ", foreignKey=" + foreignKey +
                ", childTable=" + childTable +
                '}';
    }

    public interface Builder
    {
        interface SetParentTable
        {
            SetPrimaryKey parentTable(TableName parent);
        }

        interface SetPrimaryKey
        {
            SetForeignKey primaryKey( String primaryKey );
        }

        interface SetForeignKey
        {
            SetChildTable foreignKey( String foreignKey );
        }

        interface SetChildTable
        {
            Builder childTable(TableName childTable);
        }

        Join build();
    }
}
