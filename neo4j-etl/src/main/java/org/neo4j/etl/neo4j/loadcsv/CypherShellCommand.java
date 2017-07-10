package org.neo4j.etl.neo4j.loadcsv;

import org.neo4j.etl.neo4j.loadcsv.config.CypherShellConfig;
import org.neo4j.etl.process.Commands;
import org.neo4j.etl.process.Result;

import java.nio.file.Path;

public class CypherShellCommand
{
    private final CypherShellConfig config;

    public CypherShellCommand( CypherShellConfig config )
    {
        this.config = config;
    }

    public int execute( Path loadCsvFile ) throws Exception
    {
        Commands.Builder.SetCommands builder = Commands.builder();

        config.addCommandsTo( builder );

        Commands commands = builder
                .inheritWorkingDirectory()
                .failOnNonZeroExitValue()
                .noTimeout()
                .inheritEnvironment()
                .redirectStdInFrom( loadCsvFile )
                .build();

        Result result = commands.execute().await();

        return result.exitValue();
    }
}
