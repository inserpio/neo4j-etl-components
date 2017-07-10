package org.neo4j.etl.neo4j.loadcsv.statement;

import org.neo4j.etl.neo4j.loadcsv.mapping.CsvRelationship;

import static java.lang.String.format;

public class LoadCsvForRelationshipsStatementBuilder
{
    public static String fromCsvRelationship(CsvRelationship csvRelationship, String delimiter, boolean periodicCommit )
    {
        String cypher = format(
                ( periodicCommit ? "USING PERIODIC COMMIT%n" : "" ) +
                "LOAD CSV FROM 'file:///%s/%s' AS row FIELDTERMINATOR '%s'%n" +
                "MATCH (a:%s {%s})%n" +
                "MATCH (b:%s {%s})%n" +
                "MERGE (a)-[r:%s]->(b)",
                csvRelationship.getDirectory().getFileName(),
                csvRelationship.getFile(), delimiter,
                csvRelationship.getStartNodeLabel(), csvRelationship.getStartNodeId( "row" ),
                csvRelationship.getEndNodeLabel(), csvRelationship.getEndNodeId( "row" ),
                csvRelationship.getType() );

        String relationshipAttributes = csvRelationship.getRelationshipAttributes( "row", "r" );

        if ( relationshipAttributes != null && !"".equals( relationshipAttributes ) )
        {
            cypher += format("%nSET %s", relationshipAttributes);
        }

        return cypher;
    }
}
