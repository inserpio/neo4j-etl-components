package org.neo4j.etl.neo4j.loadcsv.statement;

import org.neo4j.etl.neo4j.loadcsv.mapping.CsvNode;

import java.util.stream.Collectors;

import static java.lang.String.format;

public class CreateNodeKeyStatementBuilder
{
    public static String fromCsvNode( CsvNode csvNode )
    {
        String label = csvNode.getLabel();
        String nodeKey = csvNode.getNodeIdPropertyNames().stream()
                .map((s) -> "n." + s)
                .collect(Collectors.joining(", "));

        if ( label != null && !"".equals( label ) && nodeKey != null && !"".equals( nodeKey ) )
        {
            return format( "CREATE CONSTRAINT ON (n:%s) ASSERT (%s) IS NODE KEY", label, nodeKey );
        }
        else
        {
            return null;
        }
    }
}
