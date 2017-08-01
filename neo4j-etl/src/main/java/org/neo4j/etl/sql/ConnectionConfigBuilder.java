package org.neo4j.etl.sql;

import java.net.URI;

class ConnectionConfigBuilder implements
        ConnectionConfig.Builder.SetUsername,
        ConnectionConfig.Builder.SetPassword,
        ConnectionConfig.Builder

{
    URI uri;
    String username;
    String password;
    private String url;

    ConnectionConfigBuilder(String url) {
        this.url = url;
    }

    @Override
    public ConnectionConfig.Builder.SetPassword username(String username) {
        this.username = username;
        return this;
    }

    @Override
    public ConnectionConfig.Builder password(String password) {
        this.password = password;
        return this;
    }

    @Override
    public ConnectionConfig build() {
        this.uri = URI.create(this.url);

        return new ConnectionConfig(this);
    }
}
