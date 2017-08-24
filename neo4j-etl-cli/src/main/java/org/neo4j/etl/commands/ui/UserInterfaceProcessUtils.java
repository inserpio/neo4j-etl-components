package org.neo4j.etl.commands.ui;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;

public class UserInterfaceProcessUtils
{
    public static final String NEO4J_ETL_UI_PID_FILE_NAME = "neo4j-etl-ui.pid";

    public static void savePidToFile( long pid ) throws Exception
    {
        Path file = Paths.get( System.getProperty("java.io.tmpdir"), NEO4J_ETL_UI_PID_FILE_NAME );

        System.err.println( "Storing PID file " + file.toFile().getAbsolutePath());

        if ( file.toFile().exists() )
        {
            throw new IllegalStateException(
                    format ( "Neo4j ETL UI is already running with process ID ",
                            Files.readAllLines( file, Charset.forName( "UTF-8" ) ) ) );
        }

        List<String> lines = Arrays.asList( String.valueOf( pid ) );

        System.err.println( "Writing PID file " + lines);

        Files.write( file, lines, Charset.forName( "UTF-8" ) );
    }

    public static long loadPidFromFile() throws Exception
    {
        Path file = Paths.get( System.getProperty("java.io.tmpdir"), NEO4J_ETL_UI_PID_FILE_NAME );

        if ( !file.toFile().exists() )
        {
            throw new IllegalStateException(
                    format ( "Neo4j ETL UI is not running." ) );
        }

        List<String> lines = Files.readAllLines( file, Charset.forName( "UTF-8" ) );

        return Long.parseLong( lines.get(0) );
    }

    public static boolean deletePidFile() throws Exception
    {
        Path file = Paths.get( System.getProperty("java.io.tmpdir"), NEO4J_ETL_UI_PID_FILE_NAME );

        return file.toFile().delete();
    }
}
