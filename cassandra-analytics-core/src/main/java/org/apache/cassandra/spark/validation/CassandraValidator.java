package org.apache.cassandra.spark.validation;

import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.common.data.HealthResponse;

public class CassandraValidator implements StartupValidator
{
    private final SidecarClient sidecar;

    public CassandraValidator(SidecarClient sidecar)
    {
        this.sidecar = sidecar;
    }

    @Override
    public void validate()
    {
        HealthResponse health = sidecar.cassandraHealth().get();
        if (!health.isOk())
        {
            throw new RuntimeException("Cassandra is not healthy: " + health.status());
        }
    }
}
