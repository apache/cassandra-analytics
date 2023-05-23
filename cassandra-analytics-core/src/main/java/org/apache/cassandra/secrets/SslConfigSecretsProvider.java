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

package org.apache.cassandra.secrets;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jetbrains.annotations.NotNull;

/**
 * A {@link SecretsProvider} implementation based on the SSL configuration.
 */
public class SslConfigSecretsProvider implements SecretsProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SslConfigSecretsProvider.class);
    private final SslConfig config;

    public SslConfigSecretsProvider(SslConfig config)
    {
        this.config = config;
    }

    @Override
    public void initialize(@NotNull Map<String, String> environmentOptions)
    {
        // Do nothing
    }

    @Override
    public boolean hasKeyStoreSecrets()
    {
        String keyStorePath = config.keyStorePath();
        if (keyStorePath != null)
        {
            return Files.exists(Paths.get(keyStorePath)) && keyStorePassword() != null;
        }
        else
        {
            return config.base64EncodedKeyStore() != null && !config.base64EncodedKeyStore().isEmpty();
        }
    }

    @Override
    public InputStream keyStoreInputStream() throws IOException
    {
        if (config.keyStorePath() != null)
        {
            return Files.newInputStream(Paths.get(config.keyStorePath()));
        }
        else if (config.base64EncodedKeyStore() != null)
        {
            return new ByteArrayInputStream(Base64.getDecoder().decode(config.base64EncodedKeyStore()));
        }
        // it should never reach here
        throw new RuntimeException("keyStorePath or encodedKeyStore must be provided");
    }

    @Override
    public char[] keyStorePassword()
    {
        return Objects.requireNonNull(config.keyStorePassword(), "keyStorePassword must be not null").toCharArray();
    }

    @Override
    public String keyStoreType()
    {
        return config.keyStoreType();
    }

    @Override
    public boolean hasTrustStoreSecrets()
    {
        String trustStorePath = config.trustStorePath();
        if (trustStorePath != null)
        {
            return Files.exists(Paths.get(trustStorePath)) && trustStorePassword() != null;
        }
        return config.base64EncodedTrustStore() != null && !config.base64EncodedTrustStore().isEmpty();
    }

    @Override
    public InputStream trustStoreInputStream() throws IOException
    {
        if (config.trustStorePath() != null)
        {
            return Files.newInputStream(Paths.get(config.trustStorePath()));
        }
        else if (config.base64EncodedTrustStore() != null)
        {
            return new ByteArrayInputStream(Base64.getDecoder().decode(config.base64EncodedTrustStore()));
        }
        return null;
    }

    @Override
    public char[] trustStorePassword()
    {
        String password = config.trustStorePassword();
        return password != null ? password.toCharArray() : null;
    }

    @Override
    public String trustStoreType()
    {
        return config.trustStoreType();
    }

    @Override
    public void validateMutualTLS()
    {
        boolean fail = false;

        String keyStorePath = config.keyStorePath();
        if (keyStorePath != null)
        {
            if (!Files.exists(Paths.get(keyStorePath)))
            {
                LOGGER.warn("Provided keystore path option does not exist in the file system keystorePath={}", keyStorePath);
                fail = true;
            }
        }
        else if (config.base64EncodedKeyStore() == null || config.base64EncodedKeyStore().isEmpty())
        {
            LOGGER.warn("Neither keystore path or encoded keystore options were provided");
            fail = true;
        }

        if (keyStorePassword() == null)
        {
            LOGGER.warn("No keystore password option provided");
        }

        String trustStorePath = config.trustStorePath();
        if (trustStorePath != null && !Files.exists(Paths.get(trustStorePath)))
        {
            LOGGER.warn("Provided truststore path option does not exist in the file system trustStorePath={}", trustStorePath);
            fail = true;
        }

        if ((trustStorePath != null || config.base64EncodedTrustStore() != null) && trustStorePassword() == null)
        {
            LOGGER.warn("No truststore password option provided");
            fail = true;
        }

        if (fail)
        {
            throw new RuntimeException("No valid keystore/password provided in options");
        }
    }

    @Override
    public String secretByName(String secretName)
    {
        throw new UnsupportedOperationException("Currently not supported");
    }

    @Override
    public Path secretsPath()
    {
        return config.secretsPath() != null ? Paths.get(config.secretsPath()) : null;
    }

    @Override
    public String toString()
    {
        return "SecretsConfigProvider{"
               + "config=" + config
               + '}';
    }
}
