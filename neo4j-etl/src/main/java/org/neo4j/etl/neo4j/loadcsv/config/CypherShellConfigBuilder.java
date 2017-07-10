package org.neo4j.etl.neo4j.loadcsv.config;

import java.nio.file.Path;

class CypherShellConfigBuilder implements CypherShellConfig.Builder.SetImportToolDirectory,
        CypherShellConfig.Builder.SetNeo4jConnectionConfig,
        CypherShellConfig.Builder
{
    Path importToolDirectory;
    Neo4jConnectionConfig neo4jConnectionConfig;

    @Override
    public CypherShellConfig.Builder.SetNeo4jConnectionConfig importToolDirectory( Path directory )
    {
        this.importToolDirectory = directory;
        return this;
    }

    @Override
    public CypherShellConfig.Builder neo4jConnectionConfig(Neo4jConnectionConfig neo4jConnectionConfig)
    {
        this.neo4jConnectionConfig = neo4jConnectionConfig;
        return this;
    }
    @Override

    public CypherShellConfig build()
    {
        return new CypherShellConfig( this );
    }
}
