package org.neo4j.etl;

import com.github.rvesse.airline.Cli;
import com.github.rvesse.airline.builder.CliBuilder;
import com.github.rvesse.airline.help.Help;
import org.neo4j.etl.cli.rdbms.ImportFromRdbmsCli;
import org.neo4j.etl.cli.rdbms.GenerateMetadataMappingCli;
import org.neo4j.etl.sql.DatabaseType;
import org.neo4j.etl.util.CliRunner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.logging.LogManager;

public class NeoIntegrationCli
{
    public static void main( String[] args )
    {
        try
        {
            for ( String arg : args )
            {
                if ( arg.equalsIgnoreCase( "--debug" ) )
                {
                    LogManager.getLogManager().readConfiguration(
                            NeoIntegrationCli.class.getResourceAsStream( "/debug-logging.properties" ) );
                }
                else
                {
                    LogManager.getLogManager().readConfiguration(
                            NeoIntegrationCli.class.getResourceAsStream( "/minimal-logging.properties" ) );
                }
            }
        }
        catch ( IOException e )
        {
            System.err.println( "Error in loading configuration" );
            e.printStackTrace( System.err );
        }

        CliRunner.run( parser(), args );
    }

    static String executeMainReturnSysOut( String[] args )
    {
        PrintStream oldOut = System.out;

        ByteArrayOutputStream newOut = new ByteArrayOutputStream();
        System.setOut( new PrintStream( newOut ) );

        CliRunner.run( parser(), args, CliRunner.OnCommandFinished.DoNothing );

        try
        {
            return newOut.toString( "UTF8" );
        }
        catch ( UnsupportedEncodingException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            System.setOut( oldOut );
        }
    }

    private static Cli<Runnable> parser()
    {
        CliBuilder<Runnable> builder = Cli.<Runnable>builder( "neo4j-etl" )
                .withDescription( "Neo4j etl importers." )
                .withDefaultCommand( Help.class )
                .withCommand( Help.class );

        for (DatabaseType databaseType : DatabaseType.values())
        {
            builder.withGroup(databaseType.name().toLowerCase())
                    .withDescription(databaseType.name() + " export importers.")
                    .withDefaultCommand(Help.class)
                    .withCommand(ImportFromRdbmsCli.class)
                    .withCommand(GenerateMetadataMappingCli.class)
                    .withCommand(Help.class);
        }

        return builder.build();
    }
}
