package org.neo4j.etl.neo4j.loadcsv;

import org.neo4j.etl.neo4j.loadcsv.config.Neo4jShellConfig;
import org.neo4j.etl.process.Commands;
import org.neo4j.etl.process.Result;

public class Neo4jShellCommand
{
    private final Neo4jShellConfig config;

    public Neo4jShellCommand( Neo4jShellConfig config )
    {
        this.config = config;
    }

    public int execute() throws Exception
    {
        Commands.Builder.SetCommands builder = Commands.builder();

        config.addCommandsTo( builder );

        Commands commands = builder
                .inheritWorkingDirectory()
                .failOnNonZeroExitValue()
                .noTimeout()
                .inheritEnvironment()
                .build();

        Result result = commands.execute().await();

        return result.exitValue();
    }
}
