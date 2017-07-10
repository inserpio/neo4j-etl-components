package org.neo4j.etl.sql;

import java.net.URI;

class ConnectionConfigBuilder implements
        ConnectionConfig.Builder.SetUrl,
        ConnectionConfig.Builder.SetHost,
        ConnectionConfig.Builder.SetPort,
        ConnectionConfig.Builder.SetDatabaseOrUsername,
        ConnectionConfig.Builder.SetUsername,
        ConnectionConfig.Builder.SetPassword,
        ConnectionConfig.Builder

{
    private String url;
    private String host;
    private int port;
    private String database = "";

    DatabaseType databaseType;
    URI uri;
    String username;
    String password;

    ConnectionConfigBuilder( DatabaseType databaseType )
    {
        this.databaseType = databaseType;
    }

    @Override
    public SetUsername url( String url )
    {
        this.url = url;
        return this;
    }

    @Override
    public ConnectionConfig.Builder.SetPort host( String host )
    {
        this.host = host;
        return this;
    }

    @Override
    public SetDatabaseOrUsername port( int port )
    {
        this.port = port;
        return this;
    }

    @Override
    public SetUsername database( String database )
    {
        this.database = database;
        return this;
    }

    @Override
    public ConnectionConfig.Builder.SetPassword username( String username )
    {
        this.username = username;
        return this;
    }

    @Override
    public ConnectionConfig.Builder password( String password )
    {
        this.password = password;
        return this;
    }

    @Override
    public ConnectionConfig build()
    {
        this.uri = this.url != null ? URI.create( this.url ) : databaseType.createUri( host, port, database );

        return new ConnectionConfig( this );
    }
}
