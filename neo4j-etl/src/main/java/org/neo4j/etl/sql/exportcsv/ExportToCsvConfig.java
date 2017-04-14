package org.neo4j.etl.sql.exportcsv;

import java.nio.file.Path;

import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.sql.ConnectionConfig;
import org.neo4j.etl.util.Preconditions;

public class ExportToCsvConfig
{
    public static Builder.SetDestination builder()
    {
        return new ExportToCsvConfigBuilder();
    }

    private final Path destination;
    private final ConnectionConfig connectionConfig;
    private final Formatting formatting;

    ExportToCsvConfig( ExportToCsvConfigBuilder builder )
    {
        this.destination = Preconditions.requireNonNull( builder.destination, "Destination" );
        this.connectionConfig = Preconditions.requireNonNull( builder.connectionConfig, "ConnectionConfig" );
        this.formatting = Preconditions.requireNonNull( builder.formatting, "Formatting" );
    }

    public Path destination()
    {
        return destination;
    }

    public ConnectionConfig connectionConfig()
    {
        return connectionConfig;
    }

    public Formatting formatting()
    {
        return formatting;
    }

    public interface Builder
    {
        interface SetDestination
        {
            SetRDBMSConnectionConfig destination(Path directory );
        }

        interface SetRDBMSConnectionConfig
        {
            SetFormatting connectionConfig( ConnectionConfig config );
        }

        interface SetFormatting
        {
            Builder formatting( Formatting formatting );
        }

        ExportToCsvConfig build();
    }
}
