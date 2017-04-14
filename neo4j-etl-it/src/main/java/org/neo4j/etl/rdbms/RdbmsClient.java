package org.neo4j.etl.rdbms;

import org.neo4j.etl.sql.ConnectionConfig;
import org.neo4j.etl.sql.DatabaseClient;
import org.neo4j.etl.sql.DatabaseType;

public class RdbmsClient
{
    private DatabaseType databaseType;
    private final String host;
    private final int port;
    private final String database;
    private final String user;
    private final String password;


    public RdbmsClient( DatabaseType databaseType, String host )
    {
        this( databaseType, host, databaseType.defaultPort(), Parameters.DBName.value(), Parameters.DBUser.value(), Parameters.DBPassword.value() );
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
        DBName( "" ), DBUser( "neo4j" ), DBPassword( "neo4j" );

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
        DatabaseClient client = new DatabaseClient(
                ConnectionConfig.forDatabase( this.databaseType )
                        .host( host )
                        .port( port )
                        .database( database )
                        .username( user )
                        .password( password )
                        .build() );

        for ( String line : sql.split( ";" ) )
        {
            if ( !line.trim().isEmpty() )
            {
                client.execute( line ).await();
            }
        }
    }
}
