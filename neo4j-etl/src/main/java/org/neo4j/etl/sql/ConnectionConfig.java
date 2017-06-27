package org.neo4j.etl.sql;

import java.net.URI;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import org.neo4j.etl.util.Preconditions;

public class ConnectionConfig
{
    public static Builder.SetHost forDatabaseFromHostAndPort(DatabaseType databaseType )
    {
        return new ConnectionConfigBuilder( databaseType );
    }

    public static Builder.SetUrl forDatabaseFromUrl(DatabaseType databaseType )
    {
        return new ConnectionConfigBuilder( databaseType );
    }

    private final DatabaseType databaseType;
    private final URI uri;
    private final Credentials credentials;

    ConnectionConfig( ConnectionConfigBuilder builder )
    {
        this.databaseType = Preconditions.requireNonNull( builder.databaseType, "DatabaseType" );
        this.uri = Preconditions.requireNonNull( builder.uri, "Uri" );
        this.credentials = new Credentials(
                Preconditions.requireNonNullString( builder.username, "Username" ),
                builder.password );
    }

    public URI uri()
    {
        return this.uri;
    }

    Credentials credentials()
    {
        return credentials;
    }

    DatabaseClient.StatementFactory statementFactory()
    {
        return databaseType.statementFactory();
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
        interface SetUrl
        {
            SetUsername url ( String url);
        }

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
