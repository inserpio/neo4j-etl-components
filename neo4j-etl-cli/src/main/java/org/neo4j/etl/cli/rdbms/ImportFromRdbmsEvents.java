package org.neo4j.etl.cli.rdbms;

import java.nio.file.Path;

public interface ImportFromRdbmsEvents
{
    ImportFromRdbmsEvents EMPTY = new ImportFromRdbmsEvents()
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
