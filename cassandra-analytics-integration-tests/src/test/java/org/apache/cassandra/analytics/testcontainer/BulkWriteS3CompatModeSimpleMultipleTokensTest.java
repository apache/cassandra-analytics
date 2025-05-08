package org.apache.cassandra.analytics.testcontainer;

import org.apache.cassandra.testing.ClusterBuilderConfiguration;

public class BulkWriteS3CompatModeSimpleMultipleTokensTest extends BulkWriteS3CompatModeSimpleTest {
    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .tokenCount(4);
    }
}
