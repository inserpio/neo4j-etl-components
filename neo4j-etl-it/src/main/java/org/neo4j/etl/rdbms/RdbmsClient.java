package org.neo4j.etl.rdbms;

import org.neo4j.etl.sql.ConnectionConfig;
import org.neo4j.etl.sql.DatabaseClient;
import org.neo4j.etl.sql.DatabaseType;

import java.sql.SQLException;

public class RdbmsClient
{
    private DatabaseType databaseType;
    private String url;
    private String host;
    private int port;
    private String database;
    private final String user;
    private final String password;


    public RdbmsClient( DatabaseType databaseType, String host )
    {
        this( databaseType, host, databaseType.defaultPort(), Parameters.DBName.value(), Parameters.DBUser.value(), Parameters.DBPassword.value() );
    }

    public RdbmsClient( DatabaseType databaseType, String url, String user, String password )
    {
        this.databaseType = databaseType;
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public RdbmsClient( DatabaseType databaseType, String host, int port, String database, String user, String password )
    {
        this.databaseType = databaseType;
        this.host = host;
        this.port = port;
        this.database = database;
        this.user = user;
        this.password = password;
    }

    public enum Parameters
    {
        DBRootUser( "root" ), DBRootPassword( "xsjhdcfhsd" ), DBName( "" ), DBUser( "neo4j" ), DBPassword( "neo4j" );

        private final String value;

        Parameters( String value )
        {
            this.value = value;
        }

        public String value()
        {
            return value;
        }
    }

    public void execute( String sql ) throws Exception
    {
        execute( sql, false, -1 );
    }

    public void executeWithPeriodicCommit( String sql, int periodicCommit ) throws Exception
    {
        execute( sql, false, periodicCommit );
    }

    public void executeSkippingExceptions( String sql ) throws Exception
    {
        execute( sql, true, -1 );
    }

    private void execute( String sql, boolean skipExceptions, int periodicCommit ) throws Exception
    {
        DatabaseClient client = buildDatabaseClient();

        int index = 0;

        for ( String line : sql.split( ";" ) )
        {
            try
            {
                if (!line.trim().isEmpty())
                {
                    client.execute(line).await();

                    if (periodicCommit != -1 && ++index == periodicCommit)
                    {
                        client.close();
                        client = buildDatabaseClient();
                        index = 0;
                    }
                }
            }
            catch (Exception e)
            {
                if (!skipExceptions)
                {
                    throw e;
                }
            }
        }
    }

    private DatabaseClient buildDatabaseClient() throws SQLException, ClassNotFoundException {
        return new DatabaseClient(
                    this.url == null ?
                            ConnectionConfig.forDatabaseFromHostAndPort(databaseType)
                                    .host(host)
                                    .port(port)
                                    .database(database)
                                    .username(user)
                                    .password(password)
                                    .build()
                            :
                            ConnectionConfig.forDatabaseFromUrl(databaseType)
                                    .url(url)
                                    .username(user)
                                    .password(password)
                                    .build()
            );
    }
}
