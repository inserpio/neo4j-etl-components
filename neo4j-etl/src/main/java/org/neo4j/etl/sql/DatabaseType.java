package org.neo4j.etl.sql;

import org.neo4j.etl.neo4j.importcsv.config.formatting.QuoteChar;
import org.neo4j.etl.sql.exportcsv.formatting.SqlQuotes;

import java.net.URI;
import java.sql.ResultSet;
import java.sql.Statement;

import static java.lang.String.format;

public enum DatabaseType
{
    MySQL( "com.mysql.jdbc.Driver", 3306 )
            {
                @Override
                public URI createUri( String host, int port, String database )
                {
                    return URI.create(
                            format( "jdbc:mysql://%s:%s/%s?autoReconnect=true&useSSL=false", host, port, database ) );
                }

                @Override
                public DatabaseClient.StatementFactory statementFactory()
                {
                    return connection -> {
                        Statement statement = connection.createStatement(
                                ResultSet.TYPE_FORWARD_ONLY,
                                ResultSet.CONCUR_READ_ONLY );
                        statement.setFetchSize( Integer.MIN_VALUE );
                        return statement;
                    };
                }

                @Override
                public SqlQuotes sqlQuotes()
                {
                    return SqlQuotes.DEFAULT;
                }

                @Override
                public boolean hasSchemas() {
                    return false;
                }
            },

    PostgreSQL ( "org.postgresql.Driver", 5432 )
    {
        @Override
        public URI createUri( String host, int port, String database )
        {
            return URI.create(
                    format( "jdbc:postgresql://%s:%s/%s?ssl=false", host, port, database ) );
        }

        @Override
        public DatabaseClient.StatementFactory statementFactory()
        {
            return connection -> {
                Statement statement = connection.createStatement(
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY );
                return statement;
            };
        }

        @Override
        public SqlQuotes sqlQuotes()
        {
            return new SqlQuotes( QuoteChar.DOUBLE_QUOTES, QuoteChar.DOUBLE_QUOTES, QuoteChar.DOUBLE_QUOTES, QuoteChar.SINGLE_QUOTES );
        }

        @Override
        public boolean hasSchemas() {
            return true;
        }
    },

    Oracle ( "oracle.jdbc.OracleDriver", 1521 )
    {
        @Override
        public URI createUri( String host, int port, String database )
        {
            return URI.create(
                    format( "jdbc:oracle:thin:@%s:%s:%s", host, port, database ) );
        }

        @Override
        public DatabaseClient.StatementFactory statementFactory()
        {
            return connection -> {
                Statement statement = connection.createStatement(
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY );
                return statement;
            };
        }

        @Override
        public SqlQuotes sqlQuotes()
        {
            return new SqlQuotes( QuoteChar.DOUBLE_QUOTES, QuoteChar.DOUBLE_QUOTES, QuoteChar.DOUBLE_QUOTES, QuoteChar.SINGLE_QUOTES );
        }

        @Override
        public boolean hasSchemas() {
            return false;
        }
    };

    private final String driverClassName;
    private final int defaultPort;

    DatabaseType( String driverClassName, int defaultPort )
    {
        this.driverClassName = driverClassName;
        this.defaultPort = defaultPort;
    }

    public String driverClassName()
    {
        return driverClassName;
    }

    public int defaultPort()
    {
        return defaultPort;
    }

    public static DatabaseType fromString(String value)
    {
        for (DatabaseType databaseType : DatabaseType.values())
        {
            if (databaseType.name().equalsIgnoreCase(value))
                return databaseType;
        }
        return null;
    }

    public abstract URI createUri( String host, int port, String database );

    public abstract DatabaseClient.StatementFactory statementFactory();

    public abstract SqlQuotes sqlQuotes();

    public abstract boolean hasSchemas();
}
