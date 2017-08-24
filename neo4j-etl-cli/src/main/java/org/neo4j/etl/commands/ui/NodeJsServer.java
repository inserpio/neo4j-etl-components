package org.neo4j.etl.commands.ui;

import com.eclipsesource.v8.NodeJS;
import org.neo4j.etl.cli.ui.UserInterfaceEvents;
import org.neo4j.etl.util.CliRunner;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.nio.file.Paths;

public class NodeJsServer extends Thread
{
    private long pid;

    private NodeJS nodeJS;

    private UserInterfaceEvents events;

    public NodeJsServer( UserInterfaceEvents events )
    {
        this.events = events;
    }

    @Override
    public void run()
    {
        try
        {
            ClassLoader loader = NodeJsServer.class.getClassLoader();
            String className = this.getClass().getName().replace(".", File.separator) + ".class";
            String rootDir = loader.getResource(className).getFile();
            rootDir = rootDir.substring(rootDir.indexOf(":") + 1, rootDir.indexOf("!"));
            rootDir = rootDir.substring(0, rootDir.indexOf("lib/neo4j-etl"));

            this.pid = Long.parseLong( ManagementFactory.getRuntimeMXBean().getName().split("@")[0] );

            UserInterfaceProcessUtils.savePidToFile( this.pid );

            Path nodeScript = Paths.get( rootDir, "ui", "bin/www");

            events.onStartingServer( nodeScript );

            nodeJS = NodeJS.createNodeJS();

            nodeJS.exec( nodeScript.toFile() );

            if ( nodeJS.isRunning() )
            {
                events.onServerStarted( this.pid );
            }

            while ( nodeJS.isRunning() )
            {
                nodeJS.handleMessage();
            }
        }
        catch ( Exception e )
        {
            CliRunner.handleException( e, true );
        }
    }

    @Override
    public void interrupt()
    {
        events.onStoppingServer( this.pid );

        nodeJS.release();

        events.onServerStopped();

        super.interrupt();
    }
}
