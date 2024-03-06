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

import org.junit.jupiter.api.Test;

import org.apache.cassandra.secrets.SecretsProvider;
import org.apache.cassandra.secrets.TestSecretsProvider;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests that cover startup validation of a KeyStore
 */
public class KeyStoreValidationTests
{
    @Test
    public void testUnconfiguredKeyStore()
    {
        SecretsProvider secrets = TestSecretsProvider.notConfigured();
        KeyStoreValidation validation = new KeyStoreValidation(secrets);

        Throwable throwable = validation.perform();
        assertInstanceOf(RuntimeException.class, throwable);
        assertEquals("KeyStore is not configured", throwable.getMessage());
    }

    @Test
    public void testMissingKeyStore()
    {
        SecretsProvider secrets = TestSecretsProvider.forKeyStore("PKCS12", "keystore-missing.p12", "qwerty");
        KeyStoreValidation validation = new KeyStoreValidation(secrets);

        Throwable throwable = validation.perform();
        assertInstanceOf(RuntimeException.class, throwable);
        assertEquals("KeyStore is empty", throwable.getMessage());
    }

    @Test
    public void testMalformedKeyStore()
    {
        SecretsProvider secrets = TestSecretsProvider.forKeyStore("PKCS12", "keystore-malformed.p12", "qwerty");
        KeyStoreValidation validation = new KeyStoreValidation(secrets);

        Throwable throwable = validation.perform();
        assertInstanceOf(RuntimeException.class, throwable);
        assertEquals("KeyStore is misconfigured", throwable.getMessage());
    }

    @Test
    public void testEmptyKeyStore()
    {
        SecretsProvider secrets = TestSecretsProvider.forKeyStore("PKCS12", "keystore-empty.p12", "qwerty");
        KeyStoreValidation validation = new KeyStoreValidation(secrets);

        Throwable throwable = validation.perform();
        assertInstanceOf(RuntimeException.class, throwable);
        assertEquals("KeyStore is empty", throwable.getMessage());
    }

    @Test
    public void testInvalidKeyStore()
    {
        SecretsProvider secrets = TestSecretsProvider.forKeyStore("PKCS12", "keystore-secret.p12", "qwerty");
        KeyStoreValidation validation = new KeyStoreValidation(secrets);

        Throwable throwable = validation.perform();
        assertInstanceOf(RuntimeException.class, throwable);
        assertEquals("KeyStore contains no private keys", throwable.getMessage());
    }

    @Test
    public void testValidKeyStore()
    {
        SecretsProvider secrets = TestSecretsProvider.forKeyStore("PKCS12", "keystore-private.p12", "qwerty");
        KeyStoreValidation validation = new KeyStoreValidation(secrets);

        Throwable throwable = validation.perform();
        assertNull(throwable);
    }

    @Test
    public void testExpiredKeyStore()
    {
        SecretsProvider secrets = TestSecretsProvider.forKeyStore("PKCS12", "keystore-expired.p12", "qwerty");
        KeyStoreValidation validation = new KeyStoreValidation(secrets);

        Throwable throwable = validation.perform();
        assertInstanceOf(RuntimeException.class, throwable);
        assertThat(throwable.getMessage()).startsWith("Certificate with alias '1' is expired.");
    }
}
