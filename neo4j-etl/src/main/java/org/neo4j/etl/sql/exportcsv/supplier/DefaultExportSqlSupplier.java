package org.neo4j.etl.sql.exportcsv.supplier;

import java.util.stream.Collectors;

import org.neo4j.etl.sql.exportcsv.DatabaseExportSqlSupplier;
import org.neo4j.etl.sql.exportcsv.mapping.ColumnToCsvFieldMappings;

public class DefaultExportSqlSupplier implements DatabaseExportSqlSupplier
{
    @Override
    public String sql( ColumnToCsvFieldMappings mappings )
    {
        return "SELECT " +
                mappings.aliasedColumns().stream().collect( Collectors.joining( ", " ) ) +
                " FROM " + mappings.tableNames().stream().collect( Collectors.joining( ", " ) );
    }
}
