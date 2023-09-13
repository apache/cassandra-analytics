/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.spark.validation;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.util.Enumeration;

import org.apache.cassandra.secrets.SecretsProvider;

/**
 * A startup validation that checks the KeyStore
 */
public class KeyStoreValidation implements StartupValidation
{
    private final SecretsProvider secrets;

    public KeyStoreValidation(SecretsProvider secrets)
    {
        this.secrets = secrets;
    }

    @Override
    public void validate()
    {
        try
        {
            if (!secrets.hasKeyStoreSecrets())
            {
                throw new RuntimeException("KeyStore is unconfigured");
            }

            KeyStore keyStore = KeyStore.getInstance(secrets.keyStoreType());
            keyStore.load(secrets.keyStoreInputStream(), secrets.keyStorePassword());
            if (keyStore.size() == 0)
            {
                throw new RuntimeException("KeyStore is empty");
            }

            for (Enumeration<String> aliases = keyStore.aliases(); aliases.hasMoreElements(); )
            {
                Key key = keyStore.getKey(aliases.nextElement(), secrets.keyStorePassword());
                if (key != null && key instanceof PrivateKey)
                {
                    return;  // KeyStore contains a private key
                }
            }
            throw new RuntimeException("KeyStore contains no private keys");
        }
        catch (IOException | GeneralSecurityException exception)
        {
            throw new RuntimeException("KeyStore is misconfigured", exception);
        }
    }
}
