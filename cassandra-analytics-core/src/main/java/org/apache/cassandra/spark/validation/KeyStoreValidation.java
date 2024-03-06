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
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.function.Supplier;

import org.apache.cassandra.secrets.SecretsProvider;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.cassandra.spark.utils.Throwing;
import org.jetbrains.annotations.NotNull;

/**
 * A startup validation that checks the KeyStore
 */
public class KeyStoreValidation implements StartupValidation
{
    private final boolean configured;
    private final String type;
    private final char[] password;
    private final Supplier<InputStream> stream;

    public KeyStoreValidation(@NotNull SecretsProvider secrets)
    {
        configured = secrets.hasKeyStoreSecrets();
        type = secrets.keyStoreType();
        password = secrets.keyStorePassword();
        stream = Throwing.supplier(() -> secrets.keyStoreInputStream());
    }

    public KeyStoreValidation(@NotNull BulkSparkConf configuration)
    {
        configured = configuration.hasKeystoreAndKeystorePassword();
        type = configuration.getKeyStoreTypeOrDefault();
        password = configuration.getKeyStorePassword() == null ? null : configuration.getKeyStorePassword().toCharArray();
        stream = () -> configuration.getKeyStore();
    }

    @Override
    public void validate()
    {
        try
        {
            if (!configured)
            {
                throw new RuntimeException("KeyStore is not configured");
            }

            if (password == null)
            {
                throw new RuntimeException("Keystore password was not provided.");
            }

            KeyStore keyStore = KeyStore.getInstance(type);
            keyStore.load(stream.get(), password);
            if (keyStore.size() == 0)
            {
                throw new RuntimeException("KeyStore is empty");
            }

            for (Enumeration<String> aliases = keyStore.aliases(); aliases.hasMoreElements();)
            {
                Certificate cert = keyStore.getCertificate(aliases.nextElement());
                if (cert instanceof X509Certificate)
                {
                    ((X509Certificate) cert).checkValidity();
                }
            }

            for (Enumeration<String> aliases = keyStore.aliases(); aliases.hasMoreElements();)
            {
                Key key = keyStore.getKey(aliases.nextElement(), password);
                if (key != null && key instanceof PrivateKey)
                {
                    return;  // KeyStore contains a private key
                }
            }
            throw new RuntimeException("KeyStore contains no private keys");
        }
        catch (CertificateExpiredException exception)
        {
            throw new RuntimeException("Certificate expired. " + exception.getMessage(), exception);
        }
        catch (IOException | GeneralSecurityException exception)
        {
            throw new RuntimeException("KeyStore is misconfigured", exception);
        }
    }
}
