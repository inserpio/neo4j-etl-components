package org.neo4j.etl.util;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public class ArrayUtils
{
    public static <T> T[] prepend( T element, T[] existing )
    {
        ArrayList<T> results = new ArrayList<>( asList( existing ) );
        results.add( 0, element );

        @SuppressWarnings("unchecked")
        T[] a = (T[]) Array.newInstance( element.getClass(), results.size() );

        return results.toArray( a );
    }

    public static <T> T[] append( T element, T[] existing )
    {
        ArrayList<T> results = new ArrayList<>( asList( existing ) );
        results.add( element );

        @SuppressWarnings("unchecked")
        T[] a = (T[]) Array.newInstance( element.getClass(), results.size() );

        return results.toArray( a );
    }

    public static boolean containsIgnoreCase(List<String> list, String value)
    {
        for ( String string : list )
        {
            if ( string.equalsIgnoreCase( value ) )
            {
                return true;
            }
        }
        return false;
    }
}
