package org.neo4j.etl.util;

public class OperatingSystem
{
    public static boolean isWindows()
    {
        return System.getProperty( "os.name" ).toLowerCase().startsWith( "windows" );
    }

    public static boolean isLinux()
    {
        return System.getProperty( "os.name" ).toLowerCase().startsWith( "linux" );
    }
}
