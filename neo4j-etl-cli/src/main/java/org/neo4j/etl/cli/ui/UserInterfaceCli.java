package org.neo4j.etl.cli.ui;

import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import org.neo4j.etl.commands.ui.UserInterfaceProcessHandler;
import org.neo4j.etl.util.CliRunner;

import java.util.List;

@Command(name = "ui", description = "Starting Neo4j ETL UI Node.JS server.")
public class UserInterfaceCli implements Runnable
{
    @Arguments(description = "start / stop the neo4j-etl ui server",
            title = "start / stop server")
    protected List<String> arguments;

    @Option(type = OptionType.COMMAND,
            name = {"--debug"},
            description = "Print detailed diagnostic output.")
    protected boolean debug = false;

    @Override
    public void run()
    {
        try
        {
            if ( arguments == null || arguments.size() != 1)
            {
                wrongArguments();
            }

            String command = arguments.get(0);

            UserInterfaceProcessHandler uiProcess = new UserInterfaceProcessHandler();

            if ( command.equalsIgnoreCase( "start" ) )
            {
                uiProcess.start();
            }
            else if ( command.equalsIgnoreCase( "stop" ) )
            {
                uiProcess.stop();
            }
            else
            {
                wrongArguments();
            }
        }
        catch ( Exception e )
        {
            CliRunner.handleException( e, debug );
        }
    }

    private void wrongArguments()
    {
        throw new IllegalArgumentException("Wrong arguments. Usage: ./bin/neo4j-etl ui { start | stop }");
    }
}
