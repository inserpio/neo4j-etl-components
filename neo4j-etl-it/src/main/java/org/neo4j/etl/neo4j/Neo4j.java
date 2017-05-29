package org.neo4j.etl.neo4j;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

import org.neo4j.etl.process.Commands;
import org.neo4j.etl.process.ProcessHandle;
import org.neo4j.etl.process.ProcessLatch;
import org.neo4j.etl.process.Result;

import static org.neo4j.etl.util.OperatingSystem.isWindows;

public class Neo4j implements AutoCloseable
{
    public static final URI NEO_TX_URI = URI.create( "http://localhost:7474/db/data/transaction/commit" );

    public static final Neo4jVersion NEO4J_VERSION = Neo4jVersion.v3_1_3;

    public static final String DEFAULT_DATABASE = "graph.db";

    private final AtomicReference<ProcessHandle> processHandle = new AtomicReference<>();

    private final Path directory;

    public Neo4j( Path directory )
    {
        this.directory = directory;
    }

    public Path binDirectory()
    {
        return directory.resolve( "bin" );
    }

    public Path databasesDirectory()
    {
        return directory.resolve( "data/databases" );
    }

    public Path debugLog()
    {
        return directory.resolve( "logs/debug.log" );
    }

    public void disableAuth() throws IOException
    {
        Path confFile = directory.resolve( "conf/neo4j.conf" );

        Properties properties = new Properties();
        properties.load( Files.newInputStream( confFile ) );

        properties.setProperty( "dbms.security.auth_enabled", "false" );

        try ( BufferedWriter writer = Files.newBufferedWriter( confFile ) )
        {
            properties.store( writer, "Disabled Auth" );
        }
    }

    public boolean containsImportErrorLog( String database ) throws IOException
    {
        Path bad = databasesDirectory().resolve(database).resolve("bad.log");
        return Files.exists(bad) && Files.size(bad) > 0;
    }

    public void start() throws Exception
    {
        Result.Evaluator resultEvaluator = r ->
                (r.exitValue() == 0) ||
                        (r.exitValue() == 1 && r.stdout().startsWith( "Service is already running" ));

        if ( isWindows() )
        {
            ProcessLatch latch = new ProcessLatch( l -> l.contains( "Remote interface available at" ) );

            processHandle.set( Commands.builder( neo4jBatFile(), "console" )
                    .workingDirectory( directory )
                    .commandResultEvaluator( resultEvaluator )
                    .noTimeout()
                    .inheritEnvironment()
                    .redirectStdOutTo( latch )
                    .build()
                    .execute() );

            ProcessLatch.ProcessLatchResult result = latch.awaitContents( 20, TimeUnit.SECONDS );

            if ( !result.ok() )
            {
                throw new RuntimeException( "Unable to start Neo4j: " + result.streamContents() );
            }
        }
        else
        {
            Commands.builder( "bin/neo4j", "start" )
                    .workingDirectory( directory )
                    .commandResultEvaluator( resultEvaluator )
                    .timeout( 10, TimeUnit.SECONDS )
                    .inheritEnvironment()
                    .build()
                    .execute()
                    .await();
            Thread.sleep( 15000 );
        }
    }

    public void stop() throws Exception
    {
        if ( isWindows() )
        {
            ProcessHandle handle = this.processHandle.get();
            if ( handle != null )
            {
                Neo4jWindowsProcess neo4jWindowsProcess = new Neo4jWindowsProcess( this );

                neo4jWindowsProcess.kill();
                handle.terminate();

                processHandle.set( null );
            }
        }
        else
        {
            Commands.builder( "bin/neo4j", "stop" )
                    .workingDirectory( directory )
                    .failOnNonZeroExitValue()
                    .timeout( 10, TimeUnit.SECONDS )
                    .inheritEnvironment()
                    .build()
                    .execute()
                    .await();
        }
    }

    public String executeShell( String command ) throws Exception
    {
        Result result = Commands.builder( "bin/neo4j-shell", "-c", command )
                .workingDirectory( directory )
                .failOnNonZeroExitValue()
                .timeout( 10, TimeUnit.SECONDS )
                .inheritEnvironment()
                .build()
                .execute()
                .await();

        return result.stdout();
    }

    public String executeHttp( URI uri, String query ) throws JsonProcessingException, InterruptedException
    {
        Client client = Client.create();

        ClientResponse post = client.resource( uri )
                .type( MediaType.APPLICATION_JSON_TYPE )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .post( ClientResponse.class, requestEntityUsingJackson( query ) );

        return post.getEntity( String.class );
    }

    @Override
    public void close() throws Exception
    {
        stop();
    }

    private String requestEntityUsingJackson( String query ) throws JsonProcessingException
    {
        Statements statements = new Statements();
        statements.add( new Statement( query ) );
        return new ObjectMapper().writeValueAsString( statements );
    }

    private String neo4jBatFile()
    {
        return binDirectory().resolve( "neo4j.bat" ).toAbsolutePath().toString();
    }
}
