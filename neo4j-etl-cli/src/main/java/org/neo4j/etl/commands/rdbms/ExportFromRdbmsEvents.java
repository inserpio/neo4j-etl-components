package org.neo4j.etl.commands.rdbms;

import java.nio.file.Path;

public interface ExportFromRdbmsEvents
{
    ExportFromRdbmsEvents EMPTY = new ExportFromRdbmsEvents()
    {
        @Override
        public void onExportingToCsv( Path csvDirectory )
        {
            // Do nothing
        }

        @Override
        public void onCreatingNeo4jStore()
        {
            // Do nothing
        }

        @Override
        public void onExportComplete( Path destinationDirectory )
        {
            // Do nothing
        }
    };

    void onExportingToCsv( Path csvDirectory );

    void onCreatingNeo4jStore();

    void onExportComplete( Path destinationDirectory );
}
