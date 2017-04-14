package org.neo4j.etl.cli.rdbms;

import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.etl.commands.rdbms.ExportFromRdbmsEvents;
import org.neo4j.etl.util.CliRunner;

import static java.lang.String.format;

public class ExportRdbmsEventHandler implements ExportFromRdbmsEvents
{
    @Override
    public void onExportingToCsv( Path csvDirectory )
    {
        CliRunner.print( "Exporting from RDBMS to CSV..." );
        CliRunner.print( format( "CSV directory: %s", csvDirectory ) );
    }

    @Override
    public void onCreatingNeo4jStore()
    {
        CliRunner.print( "Creating Neo4j store from CSV..." );
    }

    @Override
    public void onExportComplete( Path destinationDirectory )
    {
        CliRunner.printResult( destinationDirectory );
        Path badLogLocation = destinationDirectory.getParent().resolve( "bad.log" );
        if ( Files.exists( badLogLocation ) )
        {
            CliRunner.print( "There were bad entries which were skipped and logged into " + badLogLocation.toString() );
        }
    }
}
