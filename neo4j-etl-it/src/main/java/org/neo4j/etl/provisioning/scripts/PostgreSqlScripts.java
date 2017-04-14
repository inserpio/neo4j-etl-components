package org.neo4j.etl.provisioning.scripts;

import org.neo4j.etl.provisioning.Script;

public class PostgreSqlScripts extends AbstractScripts
{

    public static Script startupScript()
    {
        return createScript( "/scripts/postgresql/startup.sh" );
    }

    public static Script exclusionScript()
    {
        return createScript( "/scripts/postgresql/exclusion.sql" );
    }

    public static Script northwindScript()
    {
        return createScript( "/scripts/postgresql/northwind.sql" );
    }
}
