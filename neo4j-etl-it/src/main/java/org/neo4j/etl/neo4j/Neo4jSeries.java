package org.neo4j.etl.neo4j;

import java.util.Optional;

import static java.lang.String.format;

public class Neo4jSeries
{
    public static final Neo4jSeries v1_9_x = new Neo4jSeries( 1, 9 );
    public static final Neo4jSeries v2_x_x = new Neo4jSeries( 2 );
    public static final Neo4jSeries v2_0_x = new Neo4jSeries( 2, 0 );
    public static final Neo4jSeries v2_1_x = new Neo4jSeries( 2, 1 );
    public static final Neo4jSeries v2_2_x = new Neo4jSeries( 2, 2 );
    public static final Neo4jSeries v2_3_x = new Neo4jSeries( 2, 3 );
    public static final Neo4jSeries v3_x_x = new Neo4jSeries( 3 );
    public static final Neo4jSeries v3_0_x = new Neo4jSeries( 3, 0 );
    public static final Neo4jSeries v3_1_x = new Neo4jSeries( 3, 1 );

    private final int major;
    private final Optional<Integer> minor;

    public Neo4jSeries( int major, int minor )
    {
        this( major, Optional.of( minor ) );
    }

    public Neo4jSeries( int major )
    {
        this( major, Optional.<Integer>empty() );
    }

    private Neo4jSeries( int major, Optional<Integer> minor )
    {
        this.major = major;
        this.minor = minor;
    }

    public boolean includes( Neo4jVersion version )
    {
        return major == version.major && (!minor.isPresent() || minor.get() == version.minor);
    }

    public boolean precedes( Neo4jVersion version )
    {
        return major < version.major || (major == version.major && minor.isPresent() && minor.get() < version.minor);
    }

    @Override
    public String toString()
    {
        return format( "%s.%s.x", major, minor.map( Object::toString ).orElse( "x" ) );
    }

}

