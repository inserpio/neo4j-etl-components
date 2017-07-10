package org.neo4j.etl.neo4j.loadcsv.config;

import java.nio.file.Path;

class Neo4jShellConfigBuilder implements Neo4jShellConfig.Builder.SetImportToolDirectory,
        Neo4jShellConfig.Builder.SetDestination,
        Neo4jShellConfig.Builder.SetConfig,
        Neo4jShellConfig.Builder.SetFile,
        Neo4jShellConfig.Builder
{
    Path importToolDirectory;
    Path destination;
    Path config;
    Path file;

    @Override
    public SetDestination importToolDirectory( Path directory )
    {
        this.importToolDirectory = directory;
        return this;
    }

    @Override
    public SetConfig destination( Path directory )
    {
        this.destination = directory;
        return this;
    }

    @Override
    public SetFile config( Path config )
    {
        this.config = config;
        return this;
    }

    @Override
    public Neo4jShellConfig.Builder file(Path file )
    {
        this.file = file;
        return this;
    }

    @Override
    public Neo4jShellConfig build()
    {
        return new Neo4jShellConfig( this );
    }
}
