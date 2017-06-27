package org.neo4j.etl.neo4j.importcsv.fields;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CsvFieldTest
{
    @Test
    public void shouldRoundTripStartIdToFromJson()
    {
        // given
        CsvField startId1 = CsvField.startId();
        CsvField startId2 = CsvField.startId( new IdSpace( "my-id-space" ) );

        // then
        assertEquals( startId1, CsvField.fromJson( startId1.toJson() ) );
        assertEquals( startId2, CsvField.fromJson( startId2.toJson() ) );
    }

    @Test
    public void shouldRoundTripEndIdToFromJson()
    {
        // given
        CsvField endId1 = CsvField.endId();
        CsvField endId2 = CsvField.endId( new IdSpace( "my-id-space" ) );

        // then
        assertEquals( endId1, CsvField.fromJson( endId1.toJson() ) );
        assertEquals( endId2, CsvField.fromJson( endId2.toJson() ) );
    }

    @Test
    public void shouldRoundTripRelationshipTypeToFromJson()
    {
        // given
        CsvField relationshipType = CsvField.relationshipType();

        // then
        assertEquals( relationshipType, CsvField.fromJson( relationshipType.toJson() ) );
    }

    @Test
    public void shouldRoundTripIdToFromJson()
    {
        // given
        CsvField id1 = CsvField.id();
        CsvField id2 = CsvField.id( "my-name" );
        CsvField id3 = CsvField.id( new IdSpace( "my-id-space" ) );
        CsvField id4 = CsvField.id( "my-name", new IdSpace( "my-id-space" ) );

        // then
        assertEquals( id1, CsvField.fromJson( id1.toJson() ) );
        assertEquals( id2, CsvField.fromJson( id2.toJson() ) );
        assertEquals( id3, CsvField.fromJson( id3.toJson() ) );
        assertEquals( id4, CsvField.fromJson( id4.toJson() ) );
    }

    @Test
    public void shouldRoundTripLabelToFromJson()
    {
        // given
        CsvField label = CsvField.label();

        // then
        assertEquals( label, CsvField.fromJson( label.toJson() ) );
    }

    @Test
    public void shouldRoundTripDataToFromJson()
    {
        // given
        CsvField data = CsvField.data( "my-data", Neo4jDataType.Double );

        // then
        assertEquals( data, CsvField.fromJson( data.toJson() ) );
    }

    @Test
    public void shouldRoundTripArrayToFromJson()
    {
        // given
        CsvField array = CsvField.array( "my-data", Neo4jDataType.Double );

        // then
        assertEquals( array, CsvField.fromJson( array.toJson() ) );
    }
}
