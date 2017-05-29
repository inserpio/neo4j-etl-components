package org.neo4j.etl.sql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.etl.io.AwaitHandle;
import org.neo4j.etl.sql.metadata.Schema;
import org.neo4j.etl.sql.metadata.TableName;
import org.neo4j.etl.util.FutureUtils;
import org.neo4j.etl.util.Loggers;

import static java.lang.String.format;

public class DatabaseClient implements AutoCloseable
{
    interface StatementFactory
    {
        Statement createStatement( Connection connection ) throws SQLException;
    }

    private final Connection connection;
    private final DatabaseMetaData metaData;
    private final StatementFactory statementFactory;
    private final boolean hasSchemas;

    public DatabaseClient( ConnectionConfig connectionConfig ) throws SQLException, ClassNotFoundException
    {
        Loggers.Sql.log().fine( "Connecting to database..." );

        Class.forName( connectionConfig.driverClassName() );

        try
        {
            connection = DriverManager.getConnection(
                    connectionConfig.uri().toString(),
                    connectionConfig.credentials().username(),
                    connectionConfig.credentials().password() );
        }
        catch ( SQLException e )
        {
            Loggers.Sql.log( Level.SEVERE, "Could not connect to the host database. Please check your credentials ", e);
            throw e;
        }

        metaData = connection.getMetaData();
        statementFactory = connectionConfig.statementFactory();
        hasSchemas = connectionConfig.hasSchemas();

        Loggers.Sql.log().fine( "Connected to database" );
    }

    public QueryResults primaryKeys( TableName tableName ) throws SQLException
    {
        return new SqlQueryResults( metaData.getPrimaryKeys( "", tableName.schema(), tableName.simpleName() ) );
    }

    public QueryResults foreignKeys( TableName tableName ) throws SQLException
    {
        return new SqlQueryResults( metaData.getImportedKeys( "", tableName.schema(), tableName.simpleName() ) );
    }

    public QueryResults columns( TableName tableName ) throws SQLException
    {
        return new SqlQueryResults( metaData.getColumns( "", tableName.schema(), tableName.simpleName(), null ) );
    }

    public AwaitHandle<QueryResults> executeQuery( String sql )
    {
        return new DatabaseClientAwaitHandle<>(
                FutureUtils.exceptionableFuture( () ->
                {
                    Loggers.Sql.log().finest( sql );
                    connection.setAutoCommit( false );
                    return new SqlQueryResults( statementFactory.createStatement( connection ).executeQuery( sql ) );

                }, r -> new Thread( r ).start() ) );
    }

    public AwaitHandle<Boolean> execute( String sql )
    {
        return new DatabaseClientAwaitHandle<>(
                FutureUtils.exceptionableFuture( () ->
                {
                    Loggers.Sql.log().finest( sql );
                    connection.setAutoCommit( true );
                    return connection.prepareStatement( sql ).execute();

                }, r -> new Thread( r ).start() ) );
    }

    public Collection<TableName> tables( Schema schema ) throws SQLException
    {
        Collection<TableName> tableNames = new ArrayList<>();

        String tableSchema;

        if ( schema != null && schema != Schema.UNDEFINED )
        {
            tableSchema =  StringUtils.upperCase( schema.name() );
        }
        else
        {
            tableSchema = hasSchemas ? connection.getSchema() : connection.getCatalog();
        }

        try ( ResultSet results = connection.getMetaData().getTables( null, tableSchema, null, new String[]{ "TABLE" } ) )
        {
            while ( results.next() )
            {
                tableNames.add( new TableName( tableSchema, results.getString( "TABLE_NAME" ) ) );
            }
        }

        return tableNames;
    }

    @Override
    public void close() throws Exception
    {
        connection.close();
    }

    private static class DatabaseClientAwaitHandle<T> implements AwaitHandle<T>
    {
        private final CompletableFuture<T> future;

        private DatabaseClientAwaitHandle( CompletableFuture<T> future )
        {
            this.future = future;
        }

        @Override
        public T await() throws Exception
        {
            return future.get();
        }

        @Override
        public T await( long timeout, TimeUnit unit ) throws Exception
        {
            return future.get( timeout, unit );
        }

        @Override
        public CompletableFuture<T> toFuture()
        {
            return future;
        }
    }

    private static class SqlQueryResults implements QueryResults
    {
        private final ResultSet results;

        SqlQueryResults( ResultSet results )
        {
            this.results = results;
        }

        @Override
        public boolean next() throws Exception
        {
            return results.next();
        }

        @Override
        public Stream<Map<String, String>> stream()
        {
            Collection<String> columnLabels = new ArrayList<>();

            try
            {
                ResultSetMetaData metaData = results.getMetaData();
                int columnCount = metaData.getColumnCount();
                for ( int i = 1; i <= columnCount; i++ )
                {
                    columnLabels.add( metaData.getColumnLabel( i ) );
                }
            }
            catch ( SQLException e )
            {
                throw new IllegalStateException( "Error while getting column labels from SQL result set", e );
            }

            return StreamSupport.stream( new ResultSetSpliterator( results, columnLabels ), false );
        }

        @Override
        public String getString( String columnLabel )
        {
            try
            {
                return results.getString( columnLabel );
            }
            catch ( SQLException e )
            {
                throw new RuntimeException( e );
            }
        }

        @Override
        public void close() throws Exception
        {
            results.close();
        }

        private static class ResultSetSpliterator implements Spliterator<Map<String, String>>
        {
            private final ResultSet results;
            private final Collection<String> columnLabels;

            ResultSetSpliterator( ResultSet results, Collection<String> columnLabels )
            {
                this.results = results;
                this.columnLabels = columnLabels;
            }

            @Override
            public boolean tryAdvance( Consumer<? super Map<String, String>> action )
            {
                boolean hasNext;
                try
                {
                    hasNext = results.next();
                }
                catch ( SQLException e )
                {
                    throw new IllegalStateException( "Error while iterating SQL result set", e );
                }
                if ( hasNext )
                {
                    Map<String, String> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                    for ( String columnLabel : columnLabels )
                    {
                        try
                        {
                            map.put( columnLabel, results.getString( columnLabel ) );
                        }
                        catch ( SQLException e )
                        {
                            throw new IllegalStateException(
                                    format( "Error while accessing '%s' in SQL result set", columnLabel ), e );
                        }

                    }
                    action.accept( map );
                    return true;
                }
                else
                {
                    return false;
                }
            }

            @Override
            public Spliterator<Map<String, String>> trySplit()
            {
                return null;
            }

            @Override
            public long estimateSize()
            {
                return 0;
            }

            @Override
            public int characteristics()
            {
                return 0;
            }
        }
    }
}
