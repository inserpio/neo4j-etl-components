package org.neo4j.etl.cli.rdbms;

import org.neo4j.etl.commands.rdbms.GenerateMetadataMappingEvents;
import org.neo4j.etl.util.CliRunner;

public class GenerateMetadataMappingEventHandler implements GenerateMetadataMappingEvents
{
    @Override
    public void onGeneratingMetadataMapping()
    {
        CliRunner.print( "Creating RDBMS to CSV mappings..." );
    }

    @Override
    public void onMetadataMappingGenerated()
    {
        // Do nothing
    }
}
