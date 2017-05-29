package org.neo4j.etl.provisioning;

import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

import org.neo4j.etl.process.Commands;
import org.neo4j.etl.provisioning.platforms.Aws;
import org.neo4j.etl.provisioning.platforms.Local;
import org.neo4j.etl.provisioning.platforms.TestType;
import org.neo4j.etl.provisioning.platforms.Vagrant;
import org.neo4j.etl.sql.DatabaseType;
import org.neo4j.etl.util.EnvironmentVariables;
import org.neo4j.etl.util.LazyResource;
import org.neo4j.etl.util.Resource;
import org.neo4j.etl.util.SystemProperties;

public class ServerFixture
{

    public static final String AWS_SQL_FILE_BUCKET = "https://s3-eu-west-1.amazonaws.com/etl.neo4j.com/";

    public static Resource<Server> server( String description,
                                           int port,
                                           Script script,
                                           Path directory,
                                           TestType testType )
    {
        return server( description, port, script, directory, testType, Optional.empty() );
    }

    public static Resource<Server> server( String description,
                                           int port,
                                           Script script,
                                           Path directory,
                                           TestType testType,
                                           String platform )
    {
        return server( description, port, script, directory, testType, Optional.of( platform ) );
    }

    private static Resource<Server> server( String description,
                                            int port,
                                            Script script,
                                            Path directory,
                                            final TestType testType,
                                            Optional<String> _platform )
    {
        return new LazyResource<>( new LazyResource.Lifecycle<Server>()
        {
            @Override
            public Server create() throws Exception
            {
                String platform = _platform.orElse(
                        systemPropertyOrEnvironmentVariable( "PLATFORM" ).orElse( "local" ).toLowerCase() );
                Optional<String> ec2Key = systemPropertyOrEnvironmentVariable( "EC2_SSH_KEY" );
                Optional<String> vagrantBoxUri = systemPropertyOrEnvironmentVariable( "VAGRANT_BOX_URI" );

                if ( platform.equalsIgnoreCase( "local" ) )
                {
                    return new Local().createServer( script, testType );
                }
                else if ( platform.equalsIgnoreCase( "aws" ) && ec2Key.isPresent() )
                {
                    return new Aws( description, ec2Key.get(), port ).createServer( script, testType );
                }
                else if ( vagrantBoxUri.isPresent() && !vagrantBoxUri.get().isEmpty() )
                {
                    return new Vagrant(
                            URI.create( vagrantBoxUri.get() ), directory ).createServer( script, testType );
                }
                else
                {
                    return new Vagrant( directory ).createServer( script, testType );
                }
            }

            @Override
            public void destroy( Server server ) throws Exception
            {
                server.close();
            }
        } );
    }

    private static Optional<String> systemPropertyOrEnvironmentVariable( String key )
    {
        Optional<String> value = SystemProperties.asOptionalString( key );
        return value.isPresent() ? value : EnvironmentVariables.asOptionalString( key );
    }

    public static void executeImportOfDatabase(Path tempDirectoryPath,
                                               final String databaseSqlFileName,
                                               String username,
                                               String password,
                                               String hostname,
                                               DatabaseType databaseType ) throws Exception
    {
        Client httpClient = Client.create();

        ClientResponse response = null;

        try
        {
            response = httpClient.resource( AWS_SQL_FILE_BUCKET + databaseSqlFileName ).get( ClientResponse.class );
            Path fileOnDisk = tempDirectoryPath.resolve( databaseSqlFileName );

            try ( InputStream entityInputStream = response.getEntityInputStream() )
            {
                Files.copy( entityInputStream, fileOnDisk );

                Commands commands = Commands.builder(
                        new String[]{databaseType.name().toLowerCase(), "-u", username, "-p" + password, "-h", hostname} )
                        .inheritWorkingDirectory()
                        .failOnNonZeroExitValue()
                        .noTimeout()
                        .inheritEnvironment()
                        .redirectStdInFrom( fileOnDisk )
                        .build();
                commands.execute().await( 20, TimeUnit.MINUTES );
            }
        }
        catch ( Exception e )
        {
            if ( response != null )
            {
                response.close();
            }
            throw e;
        }
    }
}
