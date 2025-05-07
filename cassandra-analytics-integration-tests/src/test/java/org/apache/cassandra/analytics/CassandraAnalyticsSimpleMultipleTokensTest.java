package org.apache.cassandra.analytics;

import org.apache.cassandra.testing.ClusterBuilderConfiguration;

public class CassandraAnalyticsSimpleMultipleTokensTest extends CassandraAnalyticsSimpleTest {
    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                .tokenCount(4);
    }
}
