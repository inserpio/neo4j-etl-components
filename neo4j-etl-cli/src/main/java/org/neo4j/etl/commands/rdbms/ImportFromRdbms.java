package org.neo4j.etl.commands.rdbms;

import org.neo4j.etl.cli.rdbms.ImportFromRdbmsEvents;
import org.neo4j.etl.environment.Environment;
import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.neo4j.loadcsv.config.Neo4jConnectionConfig;
import org.neo4j.etl.sql.ConnectionConfig;
import org.neo4j.etl.sql.exportcsv.io.TinyIntResolver;
import org.neo4j.etl.sql.exportcsv.mapping.MetadataMappings;

public interface ImportFromRdbms
{
    void extractAndLoad( ConnectionConfig rdbmsConnectionConfig,
                         Neo4jConnectionConfig neo4jConnectionConfig,
                         Environment environment,
                         Formatting formatting,
                         TinyIntResolver tinyIntResolver,
                         MetadataMappings metadataMappings,
                         ImportFromRdbmsEvents events) throws Exception;

    String quote(String providedQuote);
}
