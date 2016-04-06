package org.neo4j.integration.sql.exportcsv;

import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.integration.sql.metadata.Column;
import org.neo4j.integration.sql.metadata.ColumnRole;
import org.neo4j.integration.sql.metadata.CompositeColumn;
import org.neo4j.integration.sql.metadata.SimpleColumn;
import org.neo4j.integration.sql.metadata.SqlDataType;
import org.neo4j.integration.sql.metadata.TableName;

public class ColumnUtil
{
    public Column column( TableName table, String nameAndAlias, ColumnRole type )
    {
        return column( table, nameAndAlias, nameAndAlias, type );
    }

    public Column column( TableName table, String name, String alias, ColumnRole type )
    {
        return new SimpleColumn( table, name, alias, type, SqlDataType.TEXT );
    }

    public SimpleColumn keyColumn( TableName tableName, String nameAndAlias, ColumnRole type )
    {
        return new SimpleColumn(
                tableName,
                nameAndAlias,
                nameAndAlias,
                type,
                SqlDataType.KEY_DATA_TYPE );
    }

    public Column compositeKeyColumn( TableName tableName, List<String> columnNames, ColumnRole keyType )
    {
        return new CompositeColumn( tableName,
                columnNames.stream()
                        .map( name -> keyColumn( tableName, name, keyType ) )
                        .collect( Collectors.toList() ) );
    }
}
