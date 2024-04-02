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

import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;

/**
 * A test implementation of {@link SecretsProvider} that reads KeyStore/TrustStore from resources
 */
public final class TestSecretsProvider implements SecretsProvider
{
    private enum Kind
    {
        None,
        KeyStore,
        TrustStore
    }

    private final Kind kind;
    private final String type;
    private final String resource;
    private final String password;

    public static SecretsProvider notConfigured()
    {
        return new TestSecretsProvider(Kind.None, null, null, null);
    }

    public static SecretsProvider forKeyStore(String type, String resource, String password)
    {
        return new TestSecretsProvider(Kind.KeyStore, type, resource, password);
    }

    public static SecretsProvider forTrustStore(String type, String resource, String password)
    {
        return new TestSecretsProvider(Kind.TrustStore, type, resource, password);
    }

    private TestSecretsProvider(Kind kind, String type, String resource, String password)
    {
        this.kind = kind;
        this.type = type;
        this.resource = "/validation/" + resource;
        this.password = password;
    }

    // KeyStore

    @Override
    public boolean hasKeyStoreSecrets()
    {
        return kind == Kind.KeyStore;
    }

    @Override
    public String keyStoreType()
    {
        return kind == Kind.KeyStore ? type : null;
    }

    @Override
    public InputStream keyStoreInputStream()
    {
        return kind == Kind.KeyStore ? getClass().getResourceAsStream(resource) : null;
    }

    @Override
    public char[] keyStorePassword()
    {
        return kind == Kind.KeyStore ? password.toCharArray() : null;
    }

    // TrustStore

    @Override
    public boolean hasTrustStoreSecrets()
    {
        return kind == Kind.TrustStore;
    }

    @Override
    public String trustStoreType()
    {
        return kind == Kind.TrustStore ? type : null;
    }

    @Override
    public InputStream trustStoreInputStream()
    {
        return kind == Kind.TrustStore ? getClass().getResourceAsStream(resource) : null;
    }

    @Override
    public char[] trustStorePassword()
    {
        return kind == Kind.TrustStore ? password.toCharArray() : null;
    }

    // Miscellaneous

    @Override
    public void initialize(Map<String, String> options)
    {
    }

    @Override
    public void validateMutualTLS()
    {
    }

    @Override
    public String secretByName(String secretName)
    {
        return null;
    }

    @Override
    public Path secretsPath()
    {
        return null;
    }
}
