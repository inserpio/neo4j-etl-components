package org.neo4j.etl.commands.ui;

import org.neo4j.etl.cli.ui.UserInterfaceEventHandler;

public class UserInterfaceServer
{
    public static final void main( String args[] ) throws Exception
    {
        new NodeJsServer( new UserInterfaceEventHandler() ).start();
    }
}
