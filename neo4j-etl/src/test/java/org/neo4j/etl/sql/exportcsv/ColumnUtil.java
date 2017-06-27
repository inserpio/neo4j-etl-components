package org.neo4j.etl.sql.exportcsv;

import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.sql.metadata.Column;
import org.neo4j.etl.sql.metadata.ColumnRole;
import org.neo4j.etl.sql.metadata.ColumnValueSelectionStrategy;
import org.neo4j.etl.sql.metadata.CompositeColumn;
import org.neo4j.etl.sql.metadata.SimpleColumn;
import org.neo4j.etl.sql.metadata.SqlDataType;
import org.neo4j.etl.sql.metadata.TableName;

public class ColumnUtil
{
    public Column column( TableName table, String nameAndAlias, ColumnRole type )
    {
        return column( table, nameAndAlias, nameAndAlias, type );
    }

    public Column column( TableName table, String name, String alias, ColumnRole role )
    {
        return new SimpleColumn( table, name, alias, role, SqlDataType.TEXT,
                ColumnValueSelectionStrategy.SelectColumnValue,
                Formatting.DEFAULT );
    }

    public SimpleColumn keyColumn( TableName tableName, String nameAndAlias, ColumnRole role )
    {
        return new SimpleColumn(
                tableName,
                nameAndAlias,
                nameAndAlias,
                role,
                SqlDataType.TEXT, ColumnValueSelectionStrategy.SelectColumnValue,
                Formatting.DEFAULT );
    }

    public Column compositeKeyColumn( TableName tableName, List<String> columnNames, ColumnRole role )
    {
        return new CompositeColumn( tableName,
                columnNames.stream()
                        .map( name -> keyColumn( tableName, name, role ) )
                        .collect( Collectors.toList() ),
                role );
    }
}
