package org.apache.cassandra.spark.validation;

import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.common.data.HealthResponse;

public class SidecarValidator implements StartupValidator
{
    private final SidecarClient sidecar;

    public SidecarValidator(SidecarClient sidecar)
    {
        this.sidecar = sidecar;
    }

    @Override
    public void validate()
    {
        HealthResponse health = sidecar.sidecarHealth().get();
        if (!health.isOk())
        {
            throw new RuntimeException("Sidecar is not healthy: " + health.status());
        }
    }
}
