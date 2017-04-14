package org.neo4j.etl.provisioning.scripts;

import org.neo4j.etl.provisioning.Script;

public class MySqlScripts extends AbstractScripts
{

    public static Script startupScript()
    {
        return createScript( "/scripts/mysql/startup.sh" );
    }

    public static Script bigPerformanceStartupScript()
    {
        return createScript( "/scripts/mysql/bigperformance-startup.sh" );
    }

    public static Script musicBrainzPerformanceStartupScript()
    {
        return createScript( "/scripts/mysql/musicbrainzperformance-startup.sh" );
    }

    public static Script setupDatabaseScript()
    {
        return createScript( "/scripts/mysql/setup-db.sql" );
    }

    public static Script northwindScript()
    {
        return createScript( "/scripts/mysql/northwind.sql" );
    }

    public static Script exclusionScript()
    {
        return createScript( "/scripts/mysql/exclusion.sql" );
    }

}
