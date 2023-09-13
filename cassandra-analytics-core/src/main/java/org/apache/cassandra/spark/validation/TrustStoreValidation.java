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
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Enumeration;

import org.apache.cassandra.secrets.SecretsProvider;

/**
 * A startup validation that checks the TrustStore
 */
public class TrustStoreValidation implements StartupValidation
{
    private final SecretsProvider secrets;

    public TrustStoreValidation(SecretsProvider secrets)
    {
        this.secrets = secrets;
    }

    @Override
    public void validate()
    {
        try
        {
            if (!secrets.hasTrustStoreSecrets())
            {
                return;  // TrustStore is optional
            }

            KeyStore trustStore = KeyStore.getInstance(secrets.trustStoreType());
            trustStore.load(secrets.trustStoreInputStream(), secrets.trustStorePassword());
            if (trustStore.size() == 0)
            {
                throw new RuntimeException("TrustStore is empty");
            }

            for (Enumeration<String> aliases = trustStore.aliases(); aliases.hasMoreElements(); )
            {
                Certificate certificate = trustStore.getCertificate(aliases.nextElement());
                if (certificate != null)
                {
                    return;  // TrustStore contains a certificate
                }
            }
            throw new RuntimeException("TrustStore contains no certificates");
        }
        catch (IOException | GeneralSecurityException exception)
        {
            throw new RuntimeException("TrustStore is misconfigured", exception);
        }
    }
}
