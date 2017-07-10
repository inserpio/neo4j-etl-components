package org.neo4j.etl.commands.rdbms.importer;

import org.neo4j.etl.commands.Using;
import org.neo4j.etl.commands.rdbms.ImportFromRdbms;
import org.neo4j.etl.environment.Environment;
import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.neo4j.loadcsv.config.Neo4jConnectionConfig;
import org.neo4j.etl.neo4j.loadcsv.mapping.CsvNode;
import org.neo4j.etl.neo4j.loadcsv.mapping.CsvRelationship;
import org.neo4j.etl.sql.exportcsv.mapping.MetadataMappings;
import org.neo4j.etl.util.CypherBoltRunner;

import java.nio.file.Path;
import java.util.List;

public class BoltDriverImportFromRdbms extends AbstractLoadCsvImportFromRdbms implements ImportFromRdbms
{
    @Override
    protected void doLoadCsv( Neo4jConnectionConfig neo4jConnectionConfig,
                              Environment environment,
                              Formatting formatting,
                              MetadataMappings metadataMappings ) throws Exception
    {
        List<CsvNode> nodes = getCsvNodes( environment, formatting, metadataMappings );
        List<CsvRelationship> relationships = getCsvRelationships( environment, formatting, metadataMappings, nodes );

        Path loadCsvFile = createLoadCsvFile( environment, formatting, nodes, relationships );

        CypherBoltRunner.execute( neo4jConnectionConfig.uri(), neo4jConnectionConfig.credentials(), loadCsvFile );
    }

    @Override
    protected String begin()
    {
        return Using.BOLT_DRIVER.begin();
    }

    @Override
    protected String commit()
    {
        return Using.BOLT_DRIVER.commit();
    }

    @Override
    protected String schemaAwait()
    {
        return Using.BOLT_DRIVER.schemaAwait();
    }

    @Override
    protected boolean periodicCommit()
    {
        return Using.BOLT_DRIVER.periodicCommit();
    }
}
