package org.neo4j.integration.sql.exportcsv.io;

import java.util.Collection;
import java.util.function.BiPredicate;

import org.apache.commons.lang3.StringUtils;

import org.neo4j.integration.neo4j.importcsv.config.GraphObjectType;
import org.neo4j.integration.sql.RowAccessor;
import org.neo4j.integration.sql.metadata.Column;
import org.neo4j.integration.sql.metadata.ColumnType;

public enum RowStrategy implements BiPredicate<RowAccessor, Collection<Column>>
{
    WriteRowWithNullKey
            {
                @Override
                public boolean test( RowAccessor rowAccessor, Collection<Column> columns )
                {
                    return true;
                }
            },
    IgnoreRowWithNullKey
            {
                @Override
                public boolean test( RowAccessor row, Collection<Column> columns )
                {
                    boolean allowWriteLine = true;
                    for ( Column column : columns )
                    {
                        if ( isKeyColumn( column.type() ) )
                        {
                            if ( StringUtils.isEmpty( column.selectFrom( row ) ) )
                            {
                                allowWriteLine = false;
                                break;
                            }
                        }
                    }
                    return allowWriteLine;
                }
            };

    public static RowStrategy select( GraphObjectType graphObjectType )
    {
        if ( graphObjectType == GraphObjectType.Node )
        {
            return WriteRowWithNullKey;
        }
        else
        {
            return IgnoreRowWithNullKey;
        }
    }

    private static boolean isKeyColumn( ColumnType type )
    {
        return ColumnType.ForeignKey == type || ColumnType.PrimaryKey == type || ColumnType.CompositeKey == type;
    }
}