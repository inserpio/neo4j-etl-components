package org.neo4j.etl.sql;

import java.net.URI;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import org.neo4j.etl.util.Preconditions;

public class ConnectionConfig
{
    public static Builder.SetHost forDatabase( DatabaseType databaseType )
    {
        return new ConnectionConfigBuilder( databaseType );
    }

    private final DatabaseType databaseType;
    private final String host;
    private final int port;
    private final String database;
    private final Credentials credentials;

    ConnectionConfig( ConnectionConfigBuilder builder )
    {
        this.databaseType = Preconditions.requireNonNull( builder.databaseType, "DatabaseType" );
        this.host = Preconditions.requireNonNullString( builder.host, "Host" );
        this.port = builder.port;
        this.database = Preconditions.requireNonNull( builder.database, "Database" );
        this.credentials = new Credentials(
                Preconditions.requireNonNullString( builder.username, "Username" ),
                builder.password );
    }

    String driverClassName()
    {
        return databaseType.driverClassName();
    }

    public URI uri()
    {
        return databaseType.createUri( host, port, database );
    }

    Credentials credentials()
    {
        return credentials;
    }

    DatabaseClient.StatementFactory statementFactory()
    {
        return databaseType.statementFactory();
    }

    boolean hasSchemas()
    {
        return databaseType.hasSchemas();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals( Object o )
    {
        return EqualsBuilder.reflectionEquals( this, o );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }

    public interface Builder
    {
        interface SetHost
        {
            SetPort host( String host );
        }

        interface SetPort
        {
            SetDatabaseOrUsername port( int port );
        }

        interface SetDatabaseOrUsername
        {
            SetUsername database( String database );

            SetPassword username( String username );
        }

        interface SetUsername
        {
            SetPassword username( String username );
        }

        interface SetPassword
        {
            Builder password( String password );
        }

        ConnectionConfig build();
    }
}
