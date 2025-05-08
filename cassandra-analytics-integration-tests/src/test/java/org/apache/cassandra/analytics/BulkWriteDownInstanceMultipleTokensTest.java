package org.apache.cassandra.analytics;

import org.apache.cassandra.testing.ClusterBuilderConfiguration;

public class BulkWriteDownInstanceMultipleTokensTest extends BulkWriteDownInstanceTest {
    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .tokenCount(4);
    }
}
