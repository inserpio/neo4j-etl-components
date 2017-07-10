package org.neo4j.etl.neo4j.loadcsv.mapping;

import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.neo4j.importcsv.fields.CsvField;
import org.neo4j.etl.sql.exportcsv.mapping.ColumnToCsvFieldMappings;
import org.neo4j.etl.sql.exportcsv.mapping.MetadataMapping;
import org.neo4j.etl.sql.metadata.Column;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public class CsvNodeBuilder
{
    public static CsvNode fromMetadataMappingWithFormatting( MetadataMapping metadataMapping, Formatting formatting, Path directory )
    {
        CsvNode csvNode = new CsvNode( directory, metadataMapping.name() + ".csv" );

        ColumnToCsvFieldMappings columnToCsvFieldMappings = metadataMapping.mappings();

        List<Column> columns = columnToCsvFieldMappings.columns().stream().collect(Collectors.toList());
        List<CsvField> fields = columnToCsvFieldMappings.fields().stream().collect(Collectors.toList());

        for ( int c = 0; c < columns.size(); c++ )
        {
            CsvNodeField csvNodeField = new CsvNodeField();

            Column column = columns.get(c);
            CsvField field = fields.get(c);

            String name = "";

            if ( field.toJson().get( "name" ) != null )
            {
                name = field.toJson().get( "name" ).textValue();
            }

            boolean array = false;

            if ( field.toJson().get( "is-array" ) != null )
            {
                array = field.toJson().get( "is-array" ).booleanValue();
            }

            String space = "";

            if ( field.toJson().get( "id-space" ) != null )
            {
                space = field.toJson().get( "id-space" ).textValue();
            }

            csvNodeField.setName( formatting.propertyFormatter().format( name ) );
            csvNodeField.setType( column.sqlDataType().toNeo4jDataType() );
            csvNodeField.setArray( array );
            csvNodeField.setPosition( c );
            csvNodeField.setColumn( column.name() );
            csvNodeField.setSpace( formatting.propertyFormatter().format( space ) );
            csvNodeField.setRole( column.role() );

            csvNode.addCsvNodeField( csvNodeField );
        }

        return csvNode;
    }
}
