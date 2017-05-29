package org.neo4j.etl.sql.exportcsv.mapping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;

public class MetadataMappings implements Iterable<MetadataMapping>
{
    public static MetadataMappings fromJson( JsonNode root, Formatting formatting )
    {
        MetadataMappings metadataMappings = new MetadataMappings();

        ArrayNode array = (ArrayNode) root;
        for ( JsonNode csvResource : array )
        {
            metadataMappings.add( MetadataMapping.fromJson( csvResource, formatting ) );
        }

        return metadataMappings;
    }

    private final Collection<MetadataMapping> metadataMappings = new ArrayList<>();

    public MetadataMappings add( MetadataMapping metadataMapping )
    {
        metadataMappings.add( metadataMapping );
        return this;
    }

    @Override
    public Iterator<MetadataMapping> iterator()
    {
        return metadataMappings.iterator();
    }

    public JsonNode toJson()
    {
        ArrayNode root = JsonNodeFactory.instance.arrayNode();

        for ( MetadataMapping metadataMapping : metadataMappings )
        {
            root.add( metadataMapping.toJson() );
        }
        return root;
    }
}
