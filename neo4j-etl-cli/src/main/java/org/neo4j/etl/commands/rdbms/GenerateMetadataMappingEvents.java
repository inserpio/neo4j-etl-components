package org.neo4j.etl.commands.rdbms;

public interface GenerateMetadataMappingEvents
{
    GenerateMetadataMappingEvents EMPTY = new GenerateMetadataMappingEvents()
    {
        @Override
        public void onGeneratingMetadataMapping()
        {
            // Do nothing
        }

        @Override
        public void onMetadataMappingGenerated()
        {
            // Do nothing
        }
    };

    void onGeneratingMetadataMapping();

    void onMetadataMappingGenerated();
}
