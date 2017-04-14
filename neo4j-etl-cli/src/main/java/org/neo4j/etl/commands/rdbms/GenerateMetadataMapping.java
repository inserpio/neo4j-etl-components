package org.neo4j.etl.commands.rdbms;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.neo4j.etl.commands.DatabaseInspector;
import org.neo4j.etl.commands.SchemaExport;
import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.sql.ConnectionConfig;
import org.neo4j.etl.sql.DatabaseClient;
import org.neo4j.etl.sql.exportcsv.DatabaseExportSqlSupplier;
import org.neo4j.etl.sql.exportcsv.io.TinyIntResolver;
import org.neo4j.etl.sql.exportcsv.mapping.ExclusionMode;
import org.neo4j.etl.sql.exportcsv.mapping.FilterOptions;
import org.neo4j.etl.sql.exportcsv.mapping.MetadataMappings;
import org.neo4j.etl.sql.exportcsv.mapping.RelationshipNameResolver;

public class GenerateMetadataMapping implements Callable<MetadataMappings>
{

    private TinyIntResolver tinyIntResolver;

    public static Callable<MetadataMappings> load( String uri )
    {
        return () ->
        {
            JsonNode root = new ObjectMapper().readTree( Paths.get( uri ).toFile() );
            return MetadataMappings.fromJson( root );
        };
    }

    public static Callable<MetadataMappings> load( Reader reader ) throws IOException
    {
        JsonNode root = new ObjectMapper().readTree( reader );
        return () -> MetadataMappings.fromJson( root );
    }

    private final GenerateMetadataMappingEvents events;
    private final OutputStream output;
    private final ConnectionConfig connectionConfig;
    private final Formatting formatting;
    private final DatabaseExportSqlSupplier sqlSupplier;
    private final RelationshipNameResolver relationshipNameResolver;
    private final FilterOptions filterOptions;

    public GenerateMetadataMapping( OutputStream output,
                                    ConnectionConfig connectionConfig,
                                    Formatting formatting,
                                    DatabaseExportSqlSupplier sqlSupplier, TinyIntResolver tinyIntResolver )
    {
        this( GenerateMetadataMappingEvents.EMPTY,
                output,
                connectionConfig,
                formatting,
                sqlSupplier,
                FilterOptions.DEFAULT,
                tinyIntResolver );
    }

    public GenerateMetadataMapping( GenerateMetadataMappingEvents events,
                                    OutputStream output,
                                    ConnectionConfig connectionConfig,
                                    Formatting formatting,
                                    DatabaseExportSqlSupplier sqlSupplier,
                                    FilterOptions filterOptions, TinyIntResolver tinyIntResolver )
    {
        this.events = events;
        this.output = output;
        this.connectionConfig = connectionConfig;
        this.formatting = formatting;
        this.sqlSupplier = sqlSupplier;
        this.filterOptions = filterOptions;
        this.relationshipNameResolver = new RelationshipNameResolver( filterOptions.relationshipNameFrom() );
        this.tinyIntResolver = tinyIntResolver;
    }

    @Override
    public MetadataMappings call() throws Exception
    {
        events.onGeneratingMetadataMapping();

        DatabaseClient databaseClient = new DatabaseClient( connectionConfig );

        if ( filterOptions.exclusionMode().equals( ExclusionMode.INCLUDE ) )
        {
            filterOptions.invertTables( databaseClient.tableNames() );
        }

        SchemaExport schemaExport = new DatabaseInspector( databaseClient, formatting, filterOptions.tablesToExclude() )
                .buildSchemaExport();
        MetadataMappings metadataMappings = schemaExport
                .generateMetadataMappings( formatting, sqlSupplier, relationshipNameResolver, tinyIntResolver );

        try ( Writer writer = new OutputStreamWriter( output ) )
        {
            ObjectWriter objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();
            writer.write( objectWriter.writeValueAsString( metadataMappings.toJson() ) );
        }

        events.onMetadataMappingGenerated();

        return metadataMappings;
    }
}
