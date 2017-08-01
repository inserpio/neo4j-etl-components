package org.neo4j.etl.sql;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.neo4j.etl.util.Preconditions;

import java.net.URI;
import java.sql.ResultSet;
import java.sql.Statement;

public class ConnectionConfig {


    private final URI uri;
    private final Credentials credentials;

    ConnectionConfig(ConnectionConfigBuilder builder) {
        this.uri = Preconditions.requireNonNull(builder.uri, "Uri");
        this.credentials = new Credentials(
                Preconditions.requireNonNullString(builder.username, "Username"),
                builder.password);
    }

    public static Builder.SetUsername forDatabaseFromUrl(String url) {
        return new ConnectionConfigBuilder(url);
    }

    public URI uri() {
        return this.uri;
    }

    Credentials credentials() {
        return credentials;
    }

    DatabaseClient.StatementFactory statementFactory() {
        return connection -> {
            Statement statement = connection.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            return statement;
        };
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    public interface Builder {
        ConnectionConfig build();

        interface SetUsername {
            SetPassword username(String username);
        }

        interface SetPassword {
            Builder password(String password);
        }
    }
}
