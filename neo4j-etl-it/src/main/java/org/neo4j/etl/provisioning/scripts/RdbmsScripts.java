package org.neo4j.etl.provisioning.scripts;

import com.amazonaws.util.IOUtils;
import org.neo4j.etl.provisioning.Script;
import org.neo4j.etl.rdbms.RdbmsClient;
import org.stringtemplate.v4.ST;

import java.io.IOException;

public class RdbmsScripts {
    public static Script startupScript(String database) {
        return createScript("/scripts/" + database.toLowerCase() + "/startup.sh");
    }

    public static Script bigPerformanceStartupScript(String database) {
        return createScript("/scripts/" + database.toLowerCase() + "/bigperformance-startup.sh");
    }

    public static Script musicBrainzPerformanceStartupScript(String database) {
        return createScript("/scripts/" + database.toLowerCase() + "/musicbrainzperformance-startup.sh");
    }

    public static Script setupDatabaseScript(String database) {
        return createScript("/scripts/" + database.toLowerCase() + "/setup-db.sql");
    }

    public static Script northwindStartupScript(String database) {
        return createScript("/scripts/" + database.toLowerCase() + "/northwind-startup.sql");
    }

    public static Script northwindScript(String database) {
        return createScript("/scripts/" + database.toLowerCase() + "/northwind.sql");
    }

    public static Script exclusionStartupScript(String database) {
        return createScript("/scripts/" + database.toLowerCase() + "/exclusion-startup.sql");
    }

    public static Script exclusionScript(String database) {
        return createScript("/scripts/" + database.toLowerCase() + "/exclusion.sql");
    }

    protected static Script createScript(String path) {
        return new Script() {
            @Override
            public String value() throws IOException {
                String script = IOUtils.toString(getClass().getResourceAsStream(path));

                ST template = new ST(script);

                for (RdbmsClient.Parameters parameter : RdbmsClient.Parameters.values()) {
                    template.add(parameter.name(), parameter.value());
                }

                return template.render();
            }
        };
    }
}
