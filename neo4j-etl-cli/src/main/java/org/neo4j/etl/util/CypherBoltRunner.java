package org.neo4j.etl.util;

import org.neo4j.driver.internal.summary.InternalSummaryCounters;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.SummaryCounters;
import org.neo4j.etl.commands.Using;
import org.neo4j.etl.sql.Credentials;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.regex.Pattern;

/**
 * @author github.com/jexp
 * @since 06.07.17
 */
public class CypherBoltRunner
{
    private static final Pattern END_OF_STATEMENT = Pattern.compile( ";$", Pattern.DOTALL | Pattern.MULTILINE );

    public static SummaryCounters execute(URI uri, Credentials credentials, Path file ) throws URISyntaxException, FileNotFoundException
    {
        SummaryCounters summaryCounters = InternalSummaryCounters.EMPTY_STATS;

        Driver driver = GraphDatabase.driver( uri, AuthTokens.basic( credentials.username(), credentials.password() ) );

        Scanner scanner = new Scanner(
                new BufferedReader(
                        new FileReader( file.toAbsolutePath().toString() ) ) ).useDelimiter( END_OF_STATEMENT );

        try ( Session session = driver.session() )
        {
            Transaction tx = null;
            while ( scanner.hasNext() )
            {
                String statement = scanner.next().trim();
                if ( statement.startsWith( Using.BOLT_DRIVER.begin() ) )
                {
                    tx = session.beginTransaction();
                    statement = statement.substring( Using.BOLT_DRIVER.begin().length() );
                }
                if ( statement.startsWith( Using.BOLT_DRIVER.commit() ) )
                {
                    if ( tx != null )
                    {
                        tx.success();
                        tx.close();
                        tx = null;
                    }
                    statement = statement.substring( Using.BOLT_DRIVER.commit().length() );
                }
                if ( !statement.trim().isEmpty() )
                {
                    StatementResult result = ( tx != null ) ? tx.run( statement ) : session.run( statement );
                    summaryCounters = updateSummaryCounters(summaryCounters, result.consume());
                }
            }
        }

        printResult( summaryCounters );

        return summaryCounters;
    }

    private static SummaryCounters updateSummaryCounters( SummaryCounters summaryCounters, ResultSummary resultSummary )
    {
        SummaryCounters newCounters = resultSummary.counters();
        
        return new InternalSummaryCounters(
                summaryCounters.nodesCreated() + newCounters.nodesCreated(),
                summaryCounters.nodesDeleted() + newCounters.nodesDeleted(),
                summaryCounters.relationshipsCreated() + newCounters.relationshipsCreated(),
                summaryCounters.relationshipsDeleted() + newCounters.relationshipsDeleted(),
                summaryCounters.propertiesSet() + newCounters.propertiesSet(),
                summaryCounters.labelsAdded() + newCounters.labelsAdded(),
                summaryCounters.labelsRemoved() + newCounters.labelsRemoved(),
                summaryCounters.indexesAdded() + newCounters.indexesAdded(),
                summaryCounters.indexesRemoved() + newCounters.indexesRemoved(),
                summaryCounters.constraintsAdded() + newCounters.constraintsAdded(),
                summaryCounters.constraintsRemoved() + newCounters.constraintsRemoved()
        );
    }

    public static void printResult( SummaryCounters summary )
    {
        Loggers.Default.log( Level.INFO,
                String.format ( "(+%d,-%d) Nodes, (+%d,-%d) Labels, (+%d,-%d) Rels, (%d) Props, (+%d,-%d) Indexes, (+%d,-%d) Constraints.%n",
                        summary.nodesCreated(), summary.nodesDeleted(),
                        summary.labelsAdded(), summary.labelsRemoved(),
                        summary.relationshipsCreated(), summary.relationshipsDeleted(),
                        summary.propertiesSet(),
                        summary.indexesAdded(), summary.indexesRemoved(),
                        summary.constraintsAdded(), summary.constraintsRemoved() ) );
    }
}

