package org.neo4j.etl.commands.rdbms.importer;

import org.neo4j.etl.cli.rdbms.ImportFromRdbmsEvents;
import org.neo4j.etl.commands.rdbms.ImportFromRdbms;
import org.neo4j.etl.environment.Environment;
import org.neo4j.etl.neo4j.importcsv.config.GraphObjectType;
import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.neo4j.importcsv.config.formatting.QuoteChar;
import org.neo4j.etl.neo4j.loadcsv.config.Neo4jConnectionConfig;
import org.neo4j.etl.neo4j.loadcsv.statement.CreateUniqueConstraintStatementBuilder;
import org.neo4j.etl.neo4j.loadcsv.statement.LoadCsvForNodesStatementBuilder;
import org.neo4j.etl.neo4j.loadcsv.statement.LoadCsvForRelationshipsStatementBuilder;
import org.neo4j.etl.neo4j.loadcsv.mapping.CsvNode;
import org.neo4j.etl.neo4j.loadcsv.mapping.CsvNodeBuilder;
import org.neo4j.etl.neo4j.loadcsv.mapping.CsvRelationship;
import org.neo4j.etl.neo4j.loadcsv.mapping.CsvRelationshipBuilder;
import org.neo4j.etl.sql.ConnectionConfig;
import org.neo4j.etl.sql.exportcsv.ExportToCsvCommand;
import org.neo4j.etl.sql.exportcsv.ExportToCsvConfig;
import org.neo4j.etl.sql.exportcsv.io.TinyIntResolver;
import org.neo4j.etl.sql.exportcsv.mapping.MetadataMapping;
import org.neo4j.etl.sql.exportcsv.mapping.MetadataMappings;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

abstract class AbstractLoadCsvImportFromRdbms implements ImportFromRdbms
{
    @Override
    public void extractAndLoad( ConnectionConfig rdbmsConnectionConfig,
                                Neo4jConnectionConfig neo4jConnectionConfig,
                                Environment environment,
                                Formatting formatting,
                                TinyIntResolver tinyIntResolver,
                                MetadataMappings metadataMappings,
                                ImportFromRdbmsEvents events ) throws Exception
    {
        ExportToCsvConfig config = ExportToCsvConfig.builder()
                .destination( environment.csvDirectory() )
                .connectionConfig( rdbmsConnectionConfig )
                .formatting( formatting )
                .build();

        events.onExportingToCsv( environment.csvDirectory() );

        new ExportToCsvCommand( config, metadataMappings, tinyIntResolver ).execute();

        events.onCreatingNeo4jStore();

        doLoadCsv( neo4jConnectionConfig, environment, formatting, metadataMappings );

        events.onExportComplete( environment.destinationDirectory() );
    }

    @Override
    public String quote( String providedQuote )
    {
        return QuoteChar.DOUBLE_QUOTES.value();
    }

    protected Path createLoadCsvFile( Environment environment,
                                      Formatting formatting,
                                      List<CsvNode> nodes,
                                      List<CsvRelationship> relationships ) throws IOException
    {
        Path file = Files.createFile( Paths.get( environment.csvDirectory().toAbsolutePath().toString(), "load-csv.cypher" ) );

        BufferedWriter writer = Files.newBufferedWriter( file, Charset.forName("UTF8") );

        writer.write( begin() );
        for ( CsvNode node : nodes )
        {
            writer.write( format( CreateUniqueConstraintStatementBuilder.fromCsvNode( node ) + ";%n" ) );
        }

        writer.write( commit() );
        writer.write( schemaAwait() );

        for ( CsvNode node : nodes )
        {
            writer.write( format( LoadCsvForNodesStatementBuilder.fromCsvNode( node, formatting.delimiter().value(), periodicCommit() ) + ";%n" ) );
        }

        for ( CsvRelationship relationship : relationships )
        {
            writer.write( format( LoadCsvForRelationshipsStatementBuilder.fromCsvRelationship( relationship, formatting.delimiter().value(), periodicCommit() ) + ";%n" ) );
        }

        writer.flush();
        writer.close();

        return file;
    }

    protected List<CsvNode> getCsvNodes( Environment environment,
                                         Formatting formatting,
                                         MetadataMappings metadataMappings )
    {
        List<CsvNode> result = new ArrayList<>();

        for ( MetadataMapping metadataMapping : metadataMappings )
        {
            if ( metadataMapping.graphObjectType().equals( GraphObjectType.Node ) )
            {
                result.add( CsvNodeBuilder.fromMetadataMappingWithFormatting( metadataMapping, formatting, environment.csvDirectory() ) );
            }
        }

        return result;
    }

    protected List<CsvRelationship> getCsvRelationships( Environment environment,
                                                         Formatting formatting,
                                                         MetadataMappings metadataMappings,
                                                         List<CsvNode> nodes )
    {
        List<CsvRelationship> result = new ArrayList<>();

        for ( MetadataMapping metadataMapping : metadataMappings )
        {
            if ( metadataMapping.graphObjectType().equals( GraphObjectType.Relationship ) )
            {
                result.add( CsvRelationshipBuilder.fromMetadataMappingWithFormatting( metadataMapping, formatting, environment.csvDirectory(), nodes ) );
            }
        }

        return result;
    }

    protected abstract void doLoadCsv( Neo4jConnectionConfig neo4jConnectionConfig,
                                       Environment environment,
                                       Formatting formatting,
                                       MetadataMappings metadataMappings ) throws Exception;

    protected abstract String begin();
    protected abstract String commit();
    protected abstract String schemaAwait();
    protected abstract boolean periodicCommit();
}
