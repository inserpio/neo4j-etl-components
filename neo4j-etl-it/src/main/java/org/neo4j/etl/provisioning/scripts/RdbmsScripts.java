package org.neo4j.etl.provisioning.scripts;

import com.amazonaws.util.IOUtils;
import org.neo4j.etl.provisioning.Script;
import org.neo4j.etl.rdbms.RdbmsClient;
import org.neo4j.etl.sql.DatabaseType;
import org.stringtemplate.v4.ST;

import java.io.IOException;

public class RdbmsScripts {
    public static Script startupScript( DatabaseType database ) {
        return createScript("/scripts/" + database.name().toLowerCase() + "/startup.sh");
    }

    public static Script bigPerformanceStartupScript( DatabaseType database ) {
        return createScript("/scripts/" + database.name().toLowerCase() + "/bigperformance-startup.sh");
    }

    public static Script musicBrainzPerformanceStartupScript( DatabaseType database ) {
        return createScript("/scripts/" + database.name().toLowerCase() + "/musicbrainzperformance-startup.sh");
    }

    public static Script setupDatabaseScript( DatabaseType database ) {
        return createScript("/scripts/" + database.name().toLowerCase() + "/setup-db.sql");
    }

    public static Script northwindStartupScript( DatabaseType database ) {
        return createScript("/scripts/" + database.name().toLowerCase() + "/northwind-startup.sql");
    }

    public static Script northwindScript( DatabaseType database ) {
        return createScript("/scripts/" + database.name().toLowerCase() + "/northwind.sql");
    }

    public static Script exclusionStartupScript( DatabaseType database ) {
        return createScript("/scripts/" + database.name().toLowerCase() + "/exclusion-startup.sql");
    }

    public static Script exclusionScript( DatabaseType database ) {
        return createScript("/scripts/" + database.name().toLowerCase() + "/exclusion.sql");
    }

    protected static Script createScript( String path ) {
        return new Script() {
            @Override
            public String value() throws IOException {
                String script = IOUtils.toString(getClass().getResourceAsStream(path));

                ST template = new ST(script);

                for ( RdbmsClient.Parameters parameter : RdbmsClient.Parameters.values() )
                {
                    template.add( parameter.name(), parameter.value() );
                }

                return template.render();
            }
        };
    }
}
