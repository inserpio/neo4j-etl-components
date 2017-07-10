package org.neo4j.etl.neo4j.loadcsv.mapping;

import org.neo4j.etl.sql.metadata.ColumnRole;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CsvNode
{
    public static final String ROW_ID = "_row_id_";

    private Path directory;
    private String file;

    private List<CsvNodeField> fields;

    public CsvNode( Path directory, String file )
    {
        this.directory = directory;
        this.file = file;
        this.fields = new ArrayList<>();
    }

    public boolean addCsvNodeField( CsvNodeField field )
    {
        return this.fields.add( field );
    }

    public Path getDirectory()
    {
        return directory;
    }

    public String getFile()
    {
        return file;
    }

    public String getLabel()
    {
        for ( CsvNodeField field : fields )
        {
            if ( field.getRole().equals( ColumnRole.Literal ) )
            {
                return quote( field.getColumn().replace( "'", "" ) );
            }
        }

        return quote( "Undefined" );
    }

    public List<String> getNodeIdPropertyNames()
    {
        List<String> primaryKeyColumnNames = fields.stream()
                .filter( field -> field.getRole().equals( ColumnRole.PrimaryKey ) )
                .map( field -> field.getColumn() )
                .collect(Collectors.toList());

        List<String> nodeIdPropertyNames = fields.stream()
                .filter( field -> field.getRole().equals( ColumnRole.Data ) && primaryKeyColumnNames.contains( field.getColumn() ) )
                .map( field -> quote ( field.getName() ) )
                .collect( Collectors.toList() );

        if ( nodeIdPropertyNames.isEmpty() )
        {
            nodeIdPropertyNames.add( quote( ROW_ID ) );
        }

        return nodeIdPropertyNames;
    }

    public String getNodeId( String loadCsvRowAlias )
    {
        return parseFields( loadCsvRowAlias, "", true, true, null );
    }

    public String getNodeIdForRelationships( String loadCsvRowAlias, int position )
    {
        return parseFields( loadCsvRowAlias, "", true, true, position );
    }

    public String getNodeAttributes( String loadCsvRowAlias, String nodeAlias )
    {
        return parseFields( loadCsvRowAlias, nodeAlias, false, false, null );
    }

    private String parseFields( String loadCsvRowAlias, String nodeAlias, boolean primaryKey, boolean jsonFormat, Integer position )
    {
        String nodeId = "";

        List<String> primaryKeyColumnNames = fields.stream()
                .filter( field -> field.getRole().equals( ColumnRole.PrimaryKey ) )
                .map( field -> field.getColumn() )
                .collect( Collectors.toList() );

        if ( !primaryKey || ( primaryKey && primaryKeyColumnNames.size() == 1 ) )
        {
            nodeId = fields.stream()
                .filter( field -> field.getRole().equals( ColumnRole.Data ) && primaryKeyColumnNames.contains( field.getColumn() ) == primaryKey )
                .map( field ->
                        String.format( "%s%s%s%s[%s]",
                                ( nodeAlias != null && !"".equals( nodeAlias ) ) ? nodeAlias + "." : "",
                                quote( field.getName() ),
                                ( jsonFormat ? ": " : " = " ),
                                loadCsvRowAlias,
                                ( position != null ? position : field.getPosition() ) ) )
                .collect(Collectors.joining( ", " ));
        }

        if ( jsonFormat && "".equals( nodeId ) )
        {
            nodeId = quote( ROW_ID ) + (jsonFormat ? ": " : " = ") + loadCsvRowAlias + "[0]";
        }

        return nodeId;
    }

    public String getSpace()
    {
        return fields.stream()
                .filter( filter -> filter.getRole().equals(ColumnRole.PrimaryKey ) )
                .findFirst()
                .map( field -> field.getSpace() )
                .orElse( "" );
    }

    private String quote(String value) {
        return "`" + value + "`";
    }
}
