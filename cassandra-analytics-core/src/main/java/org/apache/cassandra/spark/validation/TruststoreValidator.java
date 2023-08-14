package org.apache.cassandra.spark.validation;

import org.apache.cassandra.secrets.SecretsProvider;

public class TruststoreValidator implements StartupValidator
{
    private final SecretsProvider secrets;

    public TruststoreValidator(SecretsProvider secrets)
    {
        this.secrets = secrets;
    }

    @Override
    public void validate()
    {
        if (!secrets.hasTrustStoreSecrets())
        {
            throw new RuntimeException("Truststore contains no secrets");
        }
    }
}
