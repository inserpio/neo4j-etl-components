package org.neo4j.etl.neo4j;

import java.util.Optional;
import java.util.regex.Matcher;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.stringtemplate.v4.ST;

import org.neo4j.etl.util.RegexUtils;

import static java.lang.Integer.parseInt;
import static java.util.regex.Pattern.quote;

public class Neo4jVersion
{
    public static final Neo4jVersion v1_9_0 = new Neo4jVersion( 1, 9, 0 );
    public static final Neo4jVersion v1_9_1 = new Neo4jVersion( 1, 9, 1 );
    public static final Neo4jVersion v1_9_2 = new Neo4jVersion( 1, 9, 2 );
    public static final Neo4jVersion v1_9_3 = new Neo4jVersion( 1, 9, 3 );
    public static final Neo4jVersion v1_9_4 = new Neo4jVersion( 1, 9, 4 );
    public static final Neo4jVersion v1_9_5 = new Neo4jVersion( 1, 9, 5 );
    public static final Neo4jVersion v1_9_6 = new Neo4jVersion( 1, 9, 6 );
    public static final Neo4jVersion v1_9_7 = new Neo4jVersion( 1, 9, 7 );
    public static final Neo4jVersion v1_9_8 = new Neo4jVersion( 1, 9, 8 );
    public static final Neo4jVersion v1_9_9 = new Neo4jVersion( 1, 9, 9 );

    public static final Neo4jVersion v2_0_0 = new Neo4jVersion( 2, 0, 0 );
    public static final Neo4jVersion v2_0_1 = new Neo4jVersion( 2, 0, 1 );
    public static final Neo4jVersion v2_0_2 = new Neo4jVersion( 2, 0, 2 );
    public static final Neo4jVersion v2_0_3 = new Neo4jVersion( 2, 0, 3 );
    public static final Neo4jVersion v2_0_4 = new Neo4jVersion( 2, 0, 4 );

    public static final Neo4jVersion v2_1_0 = new Neo4jVersion( 2, 1, 0 );
    public static final Neo4jVersion v2_1_2 = new Neo4jVersion( 2, 1, 2 );
    public static final Neo4jVersion v2_1_3 = new Neo4jVersion( 2, 1, 3 );
    public static final Neo4jVersion v2_1_4 = new Neo4jVersion( 2, 1, 4 );
    public static final Neo4jVersion v2_1_5 = new Neo4jVersion( 2, 1, 5 );
    public static final Neo4jVersion v2_1_6 = new Neo4jVersion( 2, 1, 6 );
    public static final Neo4jVersion v2_1_7 = new Neo4jVersion( 2, 1, 7 );
    public static final Neo4jVersion v2_1_8 = new Neo4jVersion( 2, 1, 8 );

    public static final Neo4jVersion v2_2_0 = new Neo4jVersion( 2, 2, 0 );
    public static final Neo4jVersion v2_2_1 = new Neo4jVersion( 2, 2, 1 );
    public static final Neo4jVersion v2_2_2 = new Neo4jVersion( 2, 2, 2 );
    public static final Neo4jVersion v2_2_3 = new Neo4jVersion( 2, 2, 3 );
    public static final Neo4jVersion v2_2_4 = new Neo4jVersion( 2, 2, 4 );
    public static final Neo4jVersion v2_2_5 = new Neo4jVersion( 2, 2, 5 );
    public static final Neo4jVersion v2_2_6 = new Neo4jVersion( 2, 2, 6 );
    public static final Neo4jVersion v2_2_7 = new Neo4jVersion( 2, 2, 7 );
    public static final Neo4jVersion v2_2_8 = new Neo4jVersion( 2, 2, 8 );

    public static final Neo4jVersion v2_3_0 = new Neo4jVersion( 2, 3, 0 );
    public static final Neo4jVersion v2_3_1 = new Neo4jVersion( 2, 3, 1 );
    public static final Neo4jVersion v2_3_2 = new Neo4jVersion( 2, 3, 2 );

    public static final Neo4jVersion v3_0_0 = new Neo4jVersion( 3, 0, 0 );
    public static final Neo4jVersion v3_0_1 = new Neo4jVersion( 3, 0, 1 );
    public static final Neo4jVersion v3_0_0_M03 = new Neo4jVersion( 3, 0, 0, Optional.of( "M03" ) );
    public static final Neo4jVersion v3_0_0_M04 = new Neo4jVersion( 3, 0, 0, Optional.of( "M04" ) );
    public static final Neo4jVersion v3_0_3 = new Neo4jVersion( 3, 0, 3 );

    public static final Neo4jVersion v3_1_0 = new Neo4jVersion( 3, 1, 0 );
    public static final Neo4jVersion v3_1_1 = new Neo4jVersion( 3, 1, 1 );
    public static final Neo4jVersion v3_1_2 = new Neo4jVersion( 3, 1, 2 );
    public static final Neo4jVersion v3_1_3 = new Neo4jVersion( 3, 1, 3 );

    final int major;
    final int minor;
    private final int patch;
    private final Optional<String> label;

    public static Optional<Neo4jVersion> lowest( Iterable<Neo4jVersion> versions )
    {
        Neo4jVersion version = null;
        for ( Neo4jVersion neo4jVersion : versions )
        {
            if ( version == null ||
                    neo4jVersion.equals( version ) ||
                    neo4jVersion.isBefore( version ) )
            {
                version = neo4jVersion;
            }
        }
        return Optional.ofNullable( version );
    }

    public static Optional<Neo4jVersion> highest( Iterable<Neo4jVersion> versions )
    {
        Neo4jVersion version = null;
        for ( Neo4jVersion neo4jVersion : versions )
        {
            if ( version == null ||
                    neo4jVersion.equals( version ) ||
                    neo4jVersion.isAfter( version ) )
            {
                version = neo4jVersion;
            }
        }
        return Optional.ofNullable( version );
    }

    public static Neo4jVersion from( String version )
    {
        return from( version, "-" );
    }

    public static Neo4jVersion from( String version, String labelSeparator )
    {
        String major = "(?<major>\\d+)";
        String minor = "\\.(?<minor>\\d+)";
        String patch = "(?:\\.(?<patch>\\d+))?";
        String label = String.format( "(?:%s(?<label>[a-zA-Z0-9]+))?", quote( labelSeparator ) );
        Matcher matcher = RegexUtils.match( version, major + minor + patch + label, "version" );
        return new Neo4jVersion(
                parseInt( matcher.group( "major" ) ),
                parseInt( matcher.group( "minor" ) ),
                parseOptionalInt( matcher.group( "patch" ) ).orElse( 0 ),
                parseOptionalString( matcher.group( "label" ) ) );
    }

    private Neo4jVersion( int major, int minor, int patch )
    {
        this( major, minor, patch, Optional.<String>empty() );
    }

    private Neo4jVersion( int major, int minor, int patch, Optional<String> label )
    {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
        this.label = label;
        if ( isBefore( 1, 9, 0 ) )
        {
            throw new IllegalArgumentException( String.format( "Neo4j versions before %s are not supported", v1_9_0 ) );
        }
    }

    public boolean isPatchRelease()
    {
        return patch != 0;
    }

    public boolean isSnapshot()
    {
        return label.isPresent() && label.get().equals( "SNAPSHOT" );
    }

    public boolean belongsTo( Neo4jSeries series )
    {
        return series.includes( this );
    }

    public boolean isInOrAfter( Neo4jSeries series )
    {
        return belongsTo( series ) || series.precedes( this );
    }

    public boolean isBefore( Neo4jVersion that )
    {
        if ( this.fullRelease().equals( that.fullRelease() ) )
        {
            if ( this.label.isPresent() && that.label.isPresent() )
            {
                return this.label.get().compareTo( that.label.get() ) < 0;
            }
            else
            {
                return this.label.isPresent();
            }
        }
        else
        {
            return isBefore( that.major, that.minor, that.patch );
        }
    }

    private Neo4jVersion fullRelease()
    {
        return new Neo4jVersion( major, minor, patch );
    }

    private boolean isBefore( int major, int minor, int release )
    {
        return this.major < major ||
                (this.major == major && this.minor < minor) ||
                (this.major == major && this.minor == minor && this.patch < release);
    }

    public boolean isAfter( Neo4jVersion that )
    {
        if ( this.fullRelease().equals( that.fullRelease() ) )
        {
            if ( this.label.isPresent() && that.label.isPresent() )
            {
                return this.label.get().compareTo( that.label.get() ) > 0;
            }
            else //noinspection SimplifiableIfStatement
                if ( !this.label.isPresent() && !that.label.isPresent() )
                {
                    return false;
                }
                else
                {
                    return !this.label.isPresent();
                }
        }
        else
        {
            return this.major > that.major
                    || (this.major == that.major && this.minor > that.minor)
                    || (this.major == that.major && this.minor == that.minor && this.patch > that.patch);
        }
    }

    public boolean equalsOrIsBefore( Neo4jVersion that )
    {
        return equals( that ) || isBefore( that );
    }

    public boolean equalsOrIsAfter( Neo4jVersion that )
    {
        return equals( that ) || isAfter( that );
    }

    public Neo4jVersion asGA()
    {
        return new Neo4jVersion( major, minor, patch );
    }

    public Neo4jVersion prerelease( String label )
    {
        if ( this.label.isPresent() )
        {
            throw new IllegalStateException( String.format( "Cannot create a pre-release of %s.", this ) );
        }
        return new Neo4jVersion( major, minor, patch, Optional.of( label ) );
    }

    public Neo4jVersion asSnapshotVersion()
    {
        return new Neo4jVersion( major, minor, patch, Optional.of( "SNAPSHOT" ) );
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals( Object o )
    {
        return EqualsBuilder.reflectionEquals( this, o );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }

    public String format( String format )
    {
        ST template = new ST( format );

        template.add( "major", major );
        template.add( "minor", minor );
        template.add( "patch", patch );
        template.add( "label", label.orElse( "" ) );

        return template.render();
    }

    @Override
    public String toString()
    {
        return toString( "-" );
    }

    public String toString( String labelSeparator )
    {
        if ( equals( v1_9_0 ) )
        {
            return format( "<major>.<minor>" );
        }
        else if ( label.isPresent() )
        {
            if ( isInOrAfter( Neo4jSeries.v3_x_x ) )
            {
                return format( String.format( "<major>.<minor>.<patch>%s<label>", labelSeparator ) );
            }
            else if ( label.get().equals( "SNAPSHOT" ) && patch == 0 )
            {
                return format( String.format( "<major>.<minor>%s<label>", labelSeparator ) );
            }
            else
            {
                return format( String.format( "<major>.<minor>.<patch>%s<label>", labelSeparator ) );
            }
        }
        else
        {
            return format( "<major>.<minor>.<patch>" );
        }
    }

    private static Optional<Integer> parseOptionalInt( String match )
    {
        return Optional.ofNullable( match ).map( Integer::parseInt );
    }

    private static Optional<String> parseOptionalString( String match )
    {
        return Optional.ofNullable( match );
    }
}
