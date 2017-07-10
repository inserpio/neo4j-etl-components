package org.neo4j.etl.neo4j.loadcsv.statement;

import org.neo4j.etl.neo4j.loadcsv.mapping.CsvNode;

import static java.lang.String.format;

public class CreateIndexStatementBuilder
{
    public static String fromCsvNode( CsvNode csvNode )
    {
        String label = csvNode.getLabel();
        String nodeId = csvNode.getNodeIdPropertyNames().size() > 0 ?
                CsvNode.ROW_ID : csvNode.getNodeIdPropertyNames().get(0);

        if ( label != null && !"".equals( label ) && nodeId != null && !"".equals( nodeId ) )
        {
            return format( "CREATE INDEX ON :%s (%s)", label, nodeId );
        }
        else
        {
            return null;
        }
    }
}
