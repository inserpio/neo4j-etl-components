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

import org.neo4j.etl.cli.rdbms.GenerateMetadataMappingEvents;
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
import org.neo4j.etl.sql.metadata.Schema;

public class GenerateMetadataMapping implements Callable<MetadataMappings>
{

    public static Callable<MetadataMappings> load( String uri, Formatting formatting )
    {
        return () ->
        {
            JsonNode root = new ObjectMapper().readTree( Paths.get( uri ).toFile() );
            return MetadataMappings.fromJson( root, formatting );
        };
    }

    public static Callable<MetadataMappings> load( Reader reader, Formatting formatting ) throws IOException
    {
        JsonNode root = new ObjectMapper().readTree( reader );
        return () -> MetadataMappings.fromJson( root, formatting );
    }

    private final GenerateMetadataMappingEvents events;
    private final OutputStream output;
    private final ConnectionConfig connectionConfig;
    private final Formatting formatting;
    private final DatabaseExportSqlSupplier sqlSupplier;
    private final RelationshipNameResolver relationshipNameResolver;
    private final FilterOptions filterOptions;
    private final TinyIntResolver tinyIntResolver;
    private Schema schema;

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
        this.schema = Schema.UNDEFINED;
    }

    public Callable<MetadataMappings> forSchema( final Schema schema ) {
        this.schema = schema;
        return this;
    }

    @Override
    public MetadataMappings call() throws Exception
    {
        events.onGeneratingMetadataMapping();

        DatabaseClient databaseClient = new DatabaseClient( connectionConfig );

        if ( filterOptions.exclusionMode().equals( ExclusionMode.INCLUDE ) )
        {
            filterOptions.invertTables( databaseClient.tables( this.schema ) );
        }

        // FIXME this should be done in a smartest way
        final Formatting formatting = Formatting.builder()
                .arrayDelimiter(this.formatting.delimiter())
                .delimiter(this.formatting.delimiter())
                .labelFormatter(this.formatting.labelFormatter())
                .propertyFormatter(this.formatting.propertyFormatter())
                .quote(this.formatting.quote())
                .relationshipFormatter(this.formatting.relationshipFormatter())
                // We use database client to set up quotes, no more hard coded quotes
                .databaseClient(databaseClient)
                .build();

        // TODO: the column types to exclude should be passed using filter options
        SchemaExport schemaExport = new DatabaseInspector( databaseClient, formatting, filterOptions )
                .buildSchemaExport( this.schema );
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
