package org.neo4j.etl.sql;

import java.net.URI;
import java.sql.ResultSet;
import java.sql.Statement;

import static java.lang.String.format;

public enum DatabaseType {
    MySQL(3306) {
        @Override
        public URI createUri(String host, int port, String database) {
            return URI.create(
                    format("jdbc:mysql://%s:%s/%s?autoReconnect=true&useSSL=false", host, port, database));
        }

        @Override
        public DatabaseClient.StatementFactory statementFactory() {
            return connection -> {
                Statement statement = connection.createStatement(
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY);
                statement.setFetchSize(Integer.MIN_VALUE);
                return statement;
            };
        }
    },

    PostgreSQL(5433) {
        @Override
        public URI createUri(String host, int port, String database) {
            return URI.create(
                    format("jdbc:postgresql://%s:%s/%s?ssl=false", host, port, database));
        }

        @Override
        public DatabaseClient.StatementFactory statementFactory() {
            return connection -> {
                Statement statement = connection.createStatement(
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY);
                return statement;
            };
        }
    },

    Oracle(1521) {
        @Override
        public URI createUri(String host, int port, String database) {
            return URI.create(
                    format("jdbc:oracle:thin:@%s:%s:%s", host, port, database));
        }

        @Override
        public DatabaseClient.StatementFactory statementFactory() {
            return connection -> {
                Statement statement = connection.createStatement(
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY);
                return statement;
            };
        }
    },

    MSSQL(1433) {
        @Override
        public URI createUri(String host, int port, String database) {
            return URI.create(
                    format("jdbc:sqlserver://%s:%s;databaseName=%s", host, port, database));
        }

        @Override
        public DatabaseClient.StatementFactory statementFactory() {
            return connection -> {
                Statement statement = connection.createStatement(
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY);
                return statement;
            };
        }
    };

    private final int defaultPort;

    DatabaseType(int defaultPort) {
        this.defaultPort = defaultPort;
    }

    public static DatabaseType fromString(String value) {
        for (DatabaseType databaseType : DatabaseType.values()) {
            if (databaseType.name().equalsIgnoreCase(value))
                return databaseType;
        }
        return null;
    }

    public int defaultPort() {
        return defaultPort;
    }

    public abstract URI createUri(String host, int port, String database);

    public abstract DatabaseClient.StatementFactory statementFactory();
}
