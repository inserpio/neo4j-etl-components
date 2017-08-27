package org.neo4j.etl.cli.ui;

import java.nio.file.Path;

public interface UserInterfaceEvents
{
    void onStartingServer( Path webappDirectory, String port );

    void onServerStarted( long pid );

    void onStoppingServer( long pid );

    void onServerStopped();
}
