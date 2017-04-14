package org.neo4j.etl.sql.exportcsv.mapping;

import java.util.LinkedHashMap;
import java.util.Map;

import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.sql.metadata.Column;

class ColumnToCsvFieldMappingsBuilder implements ColumnToCsvFieldMappings.Builder
{
    private final Map<Column, ColumnToCsvFieldMapping> mappings = new LinkedHashMap<>();
    private Formatting formatting;

    @Override
    public ColumnToCsvFieldMappings.Builder add( ColumnToCsvFieldMapping columnToCsvFieldMapping )
    {
        if ( !mappings.containsKey( columnToCsvFieldMapping.column() ) )
        {
            mappings.put( columnToCsvFieldMapping.column(), columnToCsvFieldMapping );
        }
        return this;
    }

    @Override
    public ColumnToCsvFieldMappings.Builder withFormatting(Formatting formatting )
    {
        this.formatting = formatting;
        return this;
    }

    @Override
    public ColumnToCsvFieldMappings build()
    {
        return new ColumnToCsvFieldMappings( mappings.values(), formatting );
    }
}
