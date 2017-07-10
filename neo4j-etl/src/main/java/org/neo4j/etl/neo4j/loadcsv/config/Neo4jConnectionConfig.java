package org.neo4j.etl.neo4j.loadcsv.config;

import org.neo4j.etl.sql.Credentials;

import java.net.URI;

public class Neo4jConnectionConfig
{
    private final URI uri;
    private final Credentials credentials;

    public Neo4jConnectionConfig( URI uri, String username, String password )
    {
        this.uri = uri;
        this.credentials = new Credentials( username, password );
    }

    public URI uri()
    {
        return this.uri;
    }

    public Credentials credentials()
    {
        return credentials;
    }

}
