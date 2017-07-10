package org.neo4j.etl.neo4j.loadcsv.mapping;

import org.neo4j.etl.sql.metadata.ColumnRole;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CsvRelationship
{
    private Path directory;
    private String file;
    private List<CsvRelationshipField> fields;
    private List<CsvNode> nodes;

    public CsvRelationship( Path directory, String file, List<CsvNode> nodes )
    {
        this.directory = directory;
        this.file = file;
        this.nodes = nodes;
        this.fields = new ArrayList<>();
    }

    public Path getDirectory()
    {
        return directory;
    }

    public String getFile()
    {
        return file;
    }

    public boolean addCsvRelationshipField(CsvRelationshipField field )
    {
        return this.fields.add( field );
    }

    public String getType()
    {
        for ( CsvRelationshipField field : fields )
        {
            if ( field.getRole().equals( ColumnRole.Literal ) )
            {
                return quote( field.getColumn().replace( "'", "" ) );
            }
        }

        return "`UNDEFINED`";
    }

    public String getStartNodeLabel()
    {
        return findNode("StartId", "Label", null );
    }

    public String getStartNodeId( String loadCsvRowAlias )
    {
        return findNode( "StartId", "NodeId", loadCsvRowAlias );
    }

    public String getEndNodeLabel()
    {
        return findNode( "EndId", "Label", null );
    }

    public String getEndNodeId( String loadCsvRowAlias)
    {
        return findNode( "EndId", "NodeId", loadCsvRowAlias );
    }

    private String findNode ( String columnType, String infoToRetrieve, String loadCsvRowAlias )
    {
        for ( CsvRelationshipField field : fields )
        {
            if ( ( field.getRole().equals( ColumnRole.PrimaryKey ) || field.getRole().equals( ColumnRole.ForeignKey ) ) && field.getColumnType().equals( columnType ) )
            {
                for ( CsvNode node : nodes )
                {
                    if ( node.getSpace().equals( field.getSpace() ) )
                    {
                        if ( "Label".equals( infoToRetrieve ) )
                            return node.getLabel();
                        else
                            return node.getNodeIdForRelationships( loadCsvRowAlias, field.getPosition() );
                    }
                }
            }
        }

        return null;
    }

    public String getRelationshipAttributes( String loadCsvRowAlias, String relationshipAlias )
    {
        return fields.stream()
                .filter( field -> field.getRole().equals( ColumnRole.Data ) )
                .map( field -> relationshipAlias + "." + quote( field.getName() ) + " = " + loadCsvRowAlias + "[" + field.getPosition() + "]" )
                .collect( Collectors.joining( ", " ) );
    }

    private String quote(String value) {
        return "`" + value + "`";
    }
}
