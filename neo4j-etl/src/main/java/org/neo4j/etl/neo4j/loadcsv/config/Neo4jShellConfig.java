package org.neo4j.etl.neo4j.loadcsv.config;

import org.neo4j.etl.process.Commands;
import org.neo4j.etl.process.CommandsSupplier;
import org.neo4j.etl.util.OperatingSystem;
import org.neo4j.etl.util.Preconditions;

import java.nio.file.Path;

public class Neo4jShellConfig implements CommandsSupplier
{

    public static Builder.SetImportToolDirectory builder()
    {
        return new Neo4jShellConfigBuilder();
    }

    public static final String IMPORT_TOOL = OperatingSystem.isWindows() ? "neo4j-shell.bat" : "neo4j-shell";

    private final Path importToolDirectory;
    private final Path destination;
    private final Path config;
    private final Path file;

    Neo4jShellConfig( Neo4jShellConfigBuilder builder )
    {
        this.importToolDirectory = Preconditions.requireNonNull( builder.importToolDirectory, "ImportToolDirectory" );
        this.destination = Preconditions.requireNonNull( builder.destination, "Destination" );
        this.config = Preconditions.requireNonNull( builder.config, "Config" );
        this.file = Preconditions.requireNonNull( builder.file, "File" );
    }

    @Override
    public void addCommandsTo( Commands.Builder.SetCommands commands )
    {
        commands.addCommand( importToolDirectory.resolve( IMPORT_TOOL ).toString() );

        commands.addCommand( "-v" );

        commands.addCommand( "-path" );
        commands.addCommand( destination.toAbsolutePath().toString() );

        commands.addCommand( "-config" );
        commands.addCommand( config.toAbsolutePath().toString() );

        commands.addCommand( "-file" );
        commands.addCommand( file.toAbsolutePath().toString() );
    }

    public interface Builder
    {
        interface SetImportToolDirectory
        {
            SetDestination importToolDirectory(Path directory);
        }

        interface SetDestination
        {
            SetConfig destination(Path directory);
        }

        interface SetConfig
        {
            SetFile config(Path config);
        }

        interface SetFile
        {
            Builder file(Path file);
        }

        Neo4jShellConfig build();
    }
}
