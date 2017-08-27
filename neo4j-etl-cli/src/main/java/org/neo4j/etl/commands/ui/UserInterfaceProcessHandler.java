package org.neo4j.etl.commands.ui;

import org.apache.commons.lang3.SystemUtils;
import org.neo4j.etl.util.OperatingSystem;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class UserInterfaceProcessHandler
{
    private long port;

    public UserInterfaceProcessHandler( long port )
    {
        this.port = port;
    }

    public void start() throws Exception
    {
        URL[] urls = ( ( URLClassLoader ) ClassLoader.getSystemClassLoader() ).getURLs();
        String classpath = Arrays.asList(urls)
                .stream()
                .map( url -> url.getFile() )
                .collect(Collectors.joining( System.getProperty("path.separator") ) );

        List<String> command = Arrays.asList( "java", "-cp", classpath, "org.neo4j.etl.commands.ui.UserInterfaceServer" );
        ProcessBuilder processBuilder = new ProcessBuilder( command );
        processBuilder.environment().put( "NEO4J_ETL_UI_PORT", String.valueOf( this.port ) );
        processBuilder.inheritIO();
        processBuilder.start();

        Thread.sleep( 2000 );

        String openCommand;

        if ( OperatingSystem.isWindows() )
            openCommand = "iexplorer.exe";
        else if ( OperatingSystem.isLinux() )
            openCommand = "sensible-browser";
        else
            openCommand = "open";

        ProcessBuilder browser = new ProcessBuilder( Arrays.asList( openCommand, format( "http://localhost:%s", this.port ) ) );
        browser.start();
    }

    public void stop() throws Exception
    {
        long pid = UserInterfaceProcessUtils.loadPidFromFile();

        UserInterfaceProcessUtils.deletePidFile();

        List<String> command = ( OperatingSystem.isWindows() ) ?
                Arrays.asList( "taskkill", "/PID", String.valueOf( pid ) ) :
                Arrays.asList( "kill", String.valueOf( pid ) );

        new ProcessBuilder( command ).start();
    }

}
