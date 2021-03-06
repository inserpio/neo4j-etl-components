package org.neo4j.etl.sql.exportcsv.io;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;

import org.neo4j.etl.sql.DatabaseClient;
import org.neo4j.etl.sql.QueryResults;
import org.neo4j.etl.sql.exportcsv.ExportToCsvConfig;
import org.neo4j.etl.sql.exportcsv.mapping.MetadataMapping;
import org.neo4j.etl.util.Loggers;

import static java.lang.String.format;

public class CsvFileWriter
{
    private final ExportToCsvConfig config;
    private final DatabaseClient databaseClient;
    private final ResultsToFileWriter resultsToFileWriter;

    public CsvFileWriter( ExportToCsvConfig config, DatabaseClient databaseClient, TinyIntResolver tinyIntResolver )
    {
        this.config = config;
        this.databaseClient = databaseClient;
        this.resultsToFileWriter = new ResultsToFileWriter( config.formatting(), tinyIntResolver );
    }

    public Path writeExportFile( MetadataMapping resource ) throws Exception
    {
        Loggers.Default.log( Level.INFO,
                format( "Writing CSV data for %s %s", resource.graphObjectType().name().toLowerCase(), resource.name
                        () ) );

        Path exportFile = createExportFile( resource.name() );

        try ( QueryResults results = executeSql( resource.sql() ) )
        {
            resultsToFileWriter.write( results, exportFile, resource );
        }

        return exportFile;
    }

    private Path createExportFile( String filenamePrefix ) throws IOException
    {
        Path exportFile = config.destination().resolve( format( "%s.csv", filenamePrefix ) );
        Files.createFile( exportFile );

        return exportFile;
    }

    private QueryResults executeSql( String sql ) throws Exception
    {
        return databaseClient.executeQuery( sql ).await();
    }
}
