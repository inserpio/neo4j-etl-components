package org.neo4j.etl.commands.rdbms.importer;

import org.neo4j.etl.cli.rdbms.ImportFromRdbmsEvents;
import org.neo4j.etl.commands.rdbms.ImportFromRdbms;
import org.neo4j.etl.environment.Environment;
import org.neo4j.etl.neo4j.importcsv.ImportFromCsvCommand;
import org.neo4j.etl.neo4j.importcsv.config.ImportConfig;
import org.neo4j.etl.neo4j.importcsv.config.Manifest;
import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.neo4j.importcsv.fields.IdType;
import org.neo4j.etl.neo4j.loadcsv.config.Neo4jConnectionConfig;
import org.neo4j.etl.sql.ConnectionConfig;
import org.neo4j.etl.sql.exportcsv.ExportToCsvCommand;
import org.neo4j.etl.sql.exportcsv.ExportToCsvConfig;
import org.neo4j.etl.sql.exportcsv.io.TinyIntResolver;
import org.neo4j.etl.sql.exportcsv.mapping.MetadataMappings;

public class Neo4jImportImportFromRdbms implements ImportFromRdbms
{
    @Override
    public void extractAndLoad( ConnectionConfig rdbmsConnectionConfig,
                                Neo4jConnectionConfig neo4jConnectionConfig,
                                Environment environment,
                                Formatting formatting,
                                TinyIntResolver tinyIntResolver,
                                MetadataMappings metadataMappings,
                                ImportFromRdbmsEvents events ) throws Exception
    {
        ExportToCsvConfig config = ExportToCsvConfig.builder()
                .destination( environment.csvDirectory() )
                .connectionConfig( rdbmsConnectionConfig )
                .formatting( formatting )
                .build();

        events.onExportingToCsv( environment.csvDirectory() );

        Manifest manifest = new ExportToCsvCommand( config, metadataMappings, tinyIntResolver ).execute();

        events.onCreatingNeo4jStore();

        doImport( environment, formatting, manifest );

        events.onExportComplete( environment.destinationDirectory() );
    }

    @Override
    public String quote( String providedQuote )
    {
        return providedQuote;
    }

    private void doImport( Environment environment, Formatting formatting, Manifest manifest ) throws Exception
    {
        ImportConfig.Builder builder = ImportConfig.builder()
                .importToolDirectory( environment.importToolDirectory() )
                .importToolOptions( environment.importToolOptions() )
                .destination( environment.destinationDirectory() )
                .formatting( formatting )
                .idType( IdType.String );

        manifest.addNodesAndRelationshipsToBuilder( builder );

        new ImportFromCsvCommand( builder.build() ).execute();
    }
}
