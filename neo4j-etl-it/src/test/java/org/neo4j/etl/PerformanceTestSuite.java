package org.neo4j.etl;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith( Suite.class )
@Suite.SuiteClasses({MySqlBigPerformanceTest.class, MySqlMusicBrainzPerformanceTest.class})
public class PerformanceTestSuite
{
}
