package org.neo4j.etl.neo4j.loadcsv.statement;

import org.neo4j.etl.neo4j.loadcsv.mapping.CsvNode;

import static java.lang.String.format;

public class LoadCsvForNodesStatementBuilder
{
    public static String fromCsvNode( CsvNode csvNode, String delimiter, boolean periodicCommit )
    {
        String cypher = format(
                ( periodicCommit ? "USING PERIODIC COMMIT%n" : "" ) +
                "LOAD CSV FROM 'file:///%s/%s' AS row FIELDTERMINATOR '%s'%n" +
                "MERGE (n:%s {%s})",
                csvNode.getDirectory().getFileName(),
                csvNode.getFile(), delimiter,
                csvNode.getLabel(), csvNode.getNodeId( "row" ) );

        String nodeAttributes = csvNode.getNodeAttributes( "row", "n" );

        if ( nodeAttributes != null && !"".equals( nodeAttributes ) )
        {
            cypher += format( "%nSET %s", nodeAttributes );
        }

        return cypher;
    }
}
