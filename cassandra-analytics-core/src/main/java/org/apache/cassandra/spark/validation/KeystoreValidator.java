package org.apache.cassandra.spark.validation;

import org.apache.cassandra.secrets.SecretsProvider;

public class KeystoreValidator implements StartupValidator
{
    private final SecretsProvider secrets;

    public KeystoreValidator(SecretsProvider secrets)
    {
        this.secrets = secrets;
    }

    @Override
    public void validate()
    {
        if (!secrets.hasKeyStoreSecrets())
        {
            throw new RuntimeException("Keystore contains no secrets");
        }
    }
}
