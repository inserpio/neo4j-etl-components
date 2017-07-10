package org.neo4j.etl.commands.rdbms.importer;

import org.neo4j.etl.commands.Using;
import org.neo4j.etl.commands.rdbms.ImportFromRdbms;
import org.neo4j.etl.environment.Environment;
import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.neo4j.loadcsv.CypherShellCommand;
import org.neo4j.etl.neo4j.loadcsv.config.CypherShellConfig;
import org.neo4j.etl.neo4j.loadcsv.config.Neo4jConnectionConfig;
import org.neo4j.etl.neo4j.loadcsv.mapping.CsvNode;
import org.neo4j.etl.neo4j.loadcsv.mapping.CsvRelationship;
import org.neo4j.etl.sql.exportcsv.mapping.MetadataMappings;

import java.nio.file.Path;
import java.util.List;

public class CypherShellImportFromRdbms extends AbstractLoadCsvImportFromRdbms implements ImportFromRdbms
{
    @Override
    protected void doLoadCsv( Neo4jConnectionConfig neo4jConnectionConfig,
                              Environment environment,
                              Formatting formatting,
                              MetadataMappings metadataMappings ) throws Exception
    {
        List<CsvNode> nodes = getCsvNodes( environment, formatting, metadataMappings );
        List<CsvRelationship> relationships = getCsvRelationships( environment, formatting, metadataMappings, nodes );

        Path loadCsvFile = createLoadCsvFile( environment, formatting, nodes, relationships);

        CypherShellConfig.Builder builder = CypherShellConfig.builder()
                .importToolDirectory( environment.importToolDirectory() )
                .neo4jConnectionConfig( neo4jConnectionConfig );

        new CypherShellCommand( builder.build() ).execute( loadCsvFile );
    }

    @Override
    protected String begin()
    {
        return Using.CYPHER_SHELL.begin();
    }

    @Override
    protected String commit()
    {
        return Using.CYPHER_SHELL.commit();
    }

    @Override
    protected String schemaAwait()
    {
        return Using.CYPHER_SHELL.schemaAwait();
    }

    @Override
    protected boolean periodicCommit()
    {
        return Using.CYPHER_SHELL.periodicCommit();
    }
}
