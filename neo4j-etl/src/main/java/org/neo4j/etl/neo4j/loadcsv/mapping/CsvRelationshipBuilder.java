package org.neo4j.etl.neo4j.loadcsv.mapping;

import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.neo4j.importcsv.fields.CsvField;
import org.neo4j.etl.sql.exportcsv.mapping.ColumnToCsvFieldMappings;
import org.neo4j.etl.sql.exportcsv.mapping.MetadataMapping;
import org.neo4j.etl.sql.metadata.Column;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public class CsvRelationshipBuilder
{
    public static CsvRelationship fromMetadataMappingWithFormatting(MetadataMapping metadataMapping, Formatting formatting, Path directory, List<CsvNode> nodes )
    {
        CsvRelationship csvRelationship = new CsvRelationship( directory, metadataMapping.name() + ".csv", nodes );

        ColumnToCsvFieldMappings columnToCsvFieldMappings = metadataMapping.mappings();

        List<Column> columns = columnToCsvFieldMappings.columns().stream().collect(Collectors.toList());
        List<CsvField> fields = columnToCsvFieldMappings.fields().stream().collect(Collectors.toList());

        for ( int c = 0; c < columns.size(); c++ )
        {
            CsvRelationshipField csvRelationshipField = new CsvRelationshipField();

            Column column = columns.get(c);
            CsvField field = fields.get(c);

            String name = "";

            if ( field.toJson().get( "name" ) != null )
            {
                name = field.toJson().get( "name" ).textValue();
            }

            String columnsType = "";

            if ( field.toJson().get( "type" ) != null )
            {
                columnsType = field.toJson().get( "type" ).textValue();
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

            csvRelationshipField.setName( name );
            csvRelationshipField.setColumnType( columnsType );
            csvRelationshipField.setType( column.sqlDataType().toNeo4jDataType() );
            csvRelationshipField.setArray( array );
            csvRelationshipField.setPosition( c );
            csvRelationshipField.setColumn( column.name() );
            csvRelationshipField.setSpace( formatting.propertyFormatter().format( space ) );
            csvRelationshipField.setRole( column.role() );

            csvRelationship.addCsvRelationshipField( csvRelationshipField );
        }

        return csvRelationship;
    }
}
