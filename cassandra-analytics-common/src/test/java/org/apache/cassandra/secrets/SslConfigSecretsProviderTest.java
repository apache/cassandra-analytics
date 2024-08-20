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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class SslConfigSecretsProviderTest
{
    @TempDir
    public Path folder; // CHECKSTYLE IGNORE: Public mutable field for testing

    String keyStorePassword;
    String trustStorePassword;

    @BeforeEach
    public void setup() throws IOException
    {
        Path path = folder.resolve("secrets-config");
        Files.createDirectories(path);

        try (InputStream stream = getClass().getResourceAsStream("/secrets/fakecerts/client-keystore.p12"))
        {
            Files.copy(Objects.requireNonNull(stream), path.resolve("keystore.p12"), StandardCopyOption.REPLACE_EXISTING);
        }

        try (InputStream stream = getClass().getResourceAsStream("/secrets/fakecerts/client-truststore.jks"))
        {
            Files.copy(Objects.requireNonNull(stream), path.resolve("truststore.jks"), StandardCopyOption.REPLACE_EXISTING);
        }

        keyStorePassword = readPassword("/secrets/fakecerts/client-keystore-password");
        trustStorePassword = readPassword("/secrets/fakecerts/client-truststore-password");
    }

    @Test
    public void testInitializationSucceeds()
    {
        SslConfig config = new SslConfig.Builder<>().build();
        SecretsProvider secretsConfigProvider = new SslConfigSecretsProvider(config);
        assertNotNull(secretsConfigProvider);
    }

    @Test
    public void testSuccessWithCertsPaths() throws IOException
    {
        Map<String, String> jobOptions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        jobOptions.put(SslConfig.KEYSTORE_PATH, folder.toAbsolutePath() + "/secrets-config/keystore.p12");
        jobOptions.put(SslConfig.KEYSTORE_PASSWORD, keyStorePassword);
        jobOptions.put(SslConfig.KEYSTORE_TYPE, "PKCS12");
        jobOptions.put(SslConfig.TRUSTSTORE_PATH, folder.toAbsolutePath() + "/secrets-config/truststore.jks");
        jobOptions.put(SslConfig.TRUSTSTORE_PASSWORD, trustStorePassword);
        jobOptions.put(SslConfig.TRUSTSTORE_TYPE, "JKS");

        SslConfig config = SslConfig.create(jobOptions);
        SecretsProvider provider = new SslConfigSecretsProvider(config);
        provider.initialize(Collections.emptyMap());

        assertTrue(provider.hasKeyStoreSecrets());
        long totalRead = fullyReadStream(provider.keyStoreInputStream()); // Read keyStore
        assertTrue(totalRead > 0);
        assertEquals("PKCS12", provider.keyStoreType());
        assertEquals(keyStorePassword, new String(provider.keyStorePassword()));
        assertTrue(provider.hasTrustStoreSecrets());
        totalRead = fullyReadStream(provider.trustStoreInputStream()); // Read trustStore
        assertTrue(totalRead > 0);
        assertEquals("JKS", provider.trustStoreType());
        assertEquals(trustStorePassword, new String(provider.trustStorePassword()));
        provider.validateMutualTLS();

        try
        {
            buildSslConfig(provider);
        }
        catch (GeneralSecurityException e)
        {
            fail(e.getMessage());
        }
    }

    @Test
    public void testSuccessWithEncodedCerts() throws IOException
    {
        Map<String, String> jobOptions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        String keyStorePath = folder.toAbsolutePath() + "/secrets-config/keystore.p12";
        String encodedKeyStore = Base64.getEncoder().encodeToString(Files.readAllBytes(Paths.get(keyStorePath)));
        String trustStorePath = folder.toAbsolutePath() + "/secrets-config/truststore.jks";
        String encodedTrustStore = Base64.getEncoder().encodeToString(Files.readAllBytes(Paths.get(trustStorePath)));

        jobOptions.put(SslConfig.KEYSTORE_BASE64_ENCODED, encodedKeyStore);
        jobOptions.put(SslConfig.KEYSTORE_PASSWORD, keyStorePassword);
        jobOptions.put(SslConfig.KEYSTORE_TYPE, "PKCS12");
        jobOptions.put(SslConfig.TRUSTSTORE_BASE64_ENCODED, encodedTrustStore);
        jobOptions.put(SslConfig.TRUSTSTORE_PASSWORD, trustStorePassword);
        jobOptions.put(SslConfig.TRUSTSTORE_TYPE, "JKS");

        SslConfig config = SslConfig.create(jobOptions);
        SecretsProvider provider = new SslConfigSecretsProvider(config);
        provider.initialize(Collections.emptyMap());

        assertTrue(provider.hasKeyStoreSecrets());
        long totalRead = fullyReadStream(provider.keyStoreInputStream()); // Read keyStore
        assertTrue(totalRead > 0);
        assertEquals("PKCS12", provider.keyStoreType());
        assertEquals(keyStorePassword, new String(provider.keyStorePassword()));
        assertTrue(provider.hasTrustStoreSecrets());
        totalRead = fullyReadStream(provider.trustStoreInputStream()); // Read trustStore
        assertTrue(totalRead > 0);
        assertEquals("JKS", provider.trustStoreType());
        assertEquals(trustStorePassword, new String(provider.trustStorePassword()));
        provider.validateMutualTLS();

        try
        {
            buildSslConfig(provider);
        }
        catch (GeneralSecurityException e)
        {
            fail(e.getMessage());
        }
    }

    @Test
    public void testInvalidPathToKeyStore()
    {
        Map<String, String> jobOptions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        jobOptions.put(SslConfig.KEYSTORE_PATH, folder.toAbsolutePath() + "/secrets-config/non-existent-keystore.p12");
        jobOptions.put(SslConfig.KEYSTORE_PASSWORD, keyStorePassword);
        jobOptions.put(SslConfig.KEYSTORE_TYPE, "PKCS12");
        jobOptions.put(SslConfig.TRUSTSTORE_PATH, folder.toAbsolutePath() + "/secrets-config/truststore.jks");
        jobOptions.put(SslConfig.TRUSTSTORE_PASSWORD, trustStorePassword);
        jobOptions.put(SslConfig.TRUSTSTORE_TYPE, "JKS");

        SslConfig config = SslConfig.create(jobOptions);
        SecretsProvider provider = new SslConfigSecretsProvider(config);
        provider.initialize(Collections.emptyMap());

        assertFalse(provider.hasKeyStoreSecrets());
        try
        {
            provider.validateMutualTLS();
            fail("it should fail when the path is invalid");
        }
        catch (RuntimeException e)
        {
            assertEquals("No valid keystore/password provided in options", e.getMessage());
        }
    }

    @Test
    public void testInvalidPathToTrustStore()
    {
        Map<String, String> jobOptions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        jobOptions.put(SslConfig.KEYSTORE_PATH, folder.toAbsolutePath() + "/secrets-config/keystore.p12");
        jobOptions.put(SslConfig.KEYSTORE_PASSWORD, keyStorePassword);
        jobOptions.put(SslConfig.KEYSTORE_TYPE, "PKCS12");
        jobOptions.put(SslConfig.TRUSTSTORE_PATH, folder.toAbsolutePath() + "/secrets-config/non-existent-truststore.jks");
        jobOptions.put(SslConfig.TRUSTSTORE_PASSWORD, trustStorePassword);
        jobOptions.put(SslConfig.TRUSTSTORE_TYPE, "JKS");

        SslConfig config = SslConfig.create(jobOptions);
        SecretsProvider provider = new SslConfigSecretsProvider(config);
        provider.initialize(Collections.emptyMap());

        assertFalse(provider.hasTrustStoreSecrets());
        try
        {
            provider.validateMutualTLS();
            fail("it should fail when the path is invalid");
        }
        catch (RuntimeException e)
        {
            assertEquals("No valid keystore/password provided in options", e.getMessage());
        }
    }

    private void buildSslConfig(SecretsProvider provider) throws IOException, GeneralSecurityException
    {
        provider.validateMutualTLS();

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        KeyStore keystore = KeyStore.getInstance(provider.keyStoreType());
        char[] keyStorePassAr = provider.keyStorePassword();

        if (keyStorePassAr == null)
        {
            // Support empty passwords
            keyStorePassAr = new char[]{};
        }

        try (InputStream ksf = new BufferedInputStream(provider.keyStoreInputStream()))
        {
            keystore.load(ksf, keyStorePassAr);
            kmf.init(keystore, keyStorePassAr);
        }

        // load trust store from secrets or classpath
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        KeyStore trustKeystore = KeyStore.getInstance(provider.trustStoreType());
        try (InputStream ksf = new BufferedInputStream(provider.trustStoreInputStream()))
        {
            trustKeystore.load(ksf, provider.trustStorePassword());
        }
        trustManagerFactory.init(trustKeystore);
    }

    protected long fullyReadStream(InputStream inputStream) throws IOException
    {
        long totalRead = 0;
        byte[] buffer = new byte[1024];
        try (InputStream stream = inputStream)
        {
            int read;
            while ((read = stream.read(buffer, 0, buffer.length)) != -1)
            {
                totalRead += read;
            }
        }
        return totalRead;
    }

    private String readPassword(String resourceName) throws IOException
    {
        return IOUtils.toString(Objects.requireNonNull(getClass().getResourceAsStream(resourceName)), StandardCharsets.UTF_8)
                      .trim();
    }
}
