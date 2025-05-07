package org.apache.cassandra.analytics;

import org.apache.cassandra.testing.ClusterBuilderConfiguration;

public class BulkWriteDownSidecarMultipleTokensTest extends BulkWriteDownSidecarTest {
    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                .tokenCount(4);
    }
}
