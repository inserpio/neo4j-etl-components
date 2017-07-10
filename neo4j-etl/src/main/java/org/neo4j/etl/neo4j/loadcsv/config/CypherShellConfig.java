package org.neo4j.etl.neo4j.loadcsv.config;

import org.neo4j.etl.process.Commands;
import org.neo4j.etl.process.CommandsSupplier;
import org.neo4j.etl.util.OperatingSystem;
import org.neo4j.etl.util.Preconditions;

import java.nio.file.Path;

public class CypherShellConfig implements CommandsSupplier
{
    public static final String IMPORT_TOOL = OperatingSystem.isWindows() ? "cypher-shell.bat" : "cypher-shell";

    public static Builder.SetImportToolDirectory builder()
    {
        return new CypherShellConfigBuilder();
    }

    private final Path importToolDirectory;
    private final Neo4jConnectionConfig neo4jConnectionConfig;

    CypherShellConfig( CypherShellConfigBuilder builder )
    {
        this.importToolDirectory = Preconditions.requireNonNull( builder.importToolDirectory, "ImportToolDirectory" );
        this.neo4jConnectionConfig = Preconditions.requireNonNull ( builder.neo4jConnectionConfig, "Neo4jConnectionConfig" );
    }

    @Override
    public void addCommandsTo( Commands.Builder.SetCommands commands )
    {
        commands.addCommand( importToolDirectory.resolve( IMPORT_TOOL ).toString() );

        commands.addCommand( "-a" );
        commands.addCommand( neo4jConnectionConfig.uri().toString() );

        commands.addCommand( "-u" );
        commands.addCommand( neo4jConnectionConfig.credentials().username() );

        commands.addCommand( "-p" );
        commands.addCommand( neo4jConnectionConfig.credentials().password() );

        commands.addCommand( "--encryption" );
        commands.addCommand( "false" );

        commands.addCommand( "--format" );
        commands.addCommand( "verbose" );

        commands.addCommand( "--non-interactive" );

        commands.addCommand( "--debug" );
    }

    public interface Builder
    {
        interface SetImportToolDirectory
        {
            SetNeo4jConnectionConfig importToolDirectory( Path directory );
        }

        interface SetNeo4jConnectionConfig
        {
            Builder neo4jConnectionConfig( Neo4jConnectionConfig neo4jConnectionConfig );
        }

        CypherShellConfig build();
    }
}
