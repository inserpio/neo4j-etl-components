package org.neo4j.etl.provisioning.scripts;

import com.amazonaws.util.IOUtils;
import org.neo4j.etl.provisioning.Script;
import org.neo4j.etl.rdbms.RdbmsClient;
import org.stringtemplate.v4.ST;

import java.io.IOException;

abstract class AbstractScripts
{
    protected static Script createScript( String path )
    {
        return new Script()
        {
            @Override
            public String value() throws IOException
            {
                String script = IOUtils.toString( getClass().getResourceAsStream( path ) );

                ST template = new ST( script );

                for ( RdbmsClient.Parameters parameter : RdbmsClient.Parameters.values() )
                {
                    template.add( parameter.name(), parameter.value() );
                }

                return template.render();
            }
        };
    }
}
