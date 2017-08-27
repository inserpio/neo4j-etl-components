package org.neo4j.etl.cli.ui;

import org.neo4j.etl.util.CliRunner;

import java.nio.file.Path;

import static java.lang.String.format;

public class UserInterfaceEventHandler implements UserInterfaceEvents
{
    @Override
    public void onStartingServer( Path webappDirectory, String port )
    {
        CliRunner.print( format( "Starting Node.JS server on port %s...", port ) );
        CliRunner.print( format( "Webapp directory: %s", webappDirectory ) );
    }

    @Override
    public void onServerStarted( long pid )
    {
        CliRunner.print( format( "Node.JS server successfully started (pid: %s)", pid ) );
    }

    @Override
    public void onStoppingServer( long pid )
    {
        CliRunner.print( format( "Stopping Node.JS Server (pid: %s)...", pid ) );
    }

    @Override
    public void onServerStopped()
    {
        CliRunner.print( "Node.JS Server stopped." );
    }
}
