package org.neo4j.etl.commands.ui;

import org.neo4j.etl.util.OperatingSystem;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class UserInterfaceProcessHandler
{
    public void start() throws Exception
    {
        URL[] urls = ( ( URLClassLoader ) ClassLoader.getSystemClassLoader() ).getURLs();
        String classpath = Arrays.asList(urls)
                .stream()
                .map( url -> url.getFile() )
                .collect(Collectors.joining( System.getProperty("path.separator") ) );
        List<String> command = Arrays.asList( "java", "-cp", classpath, "org.neo4j.etl.commands.ui.UserInterfaceServer" );
        ProcessBuilder processBuilder = new ProcessBuilder( command );
        processBuilder.inheritIO();
        processBuilder.start();
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
