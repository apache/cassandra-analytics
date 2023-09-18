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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.secrets.SecretsProvider;
import org.apache.cassandra.secrets.TestSecretsProvider;

/**
 * Unit tests that cover startup validation of a TrustStore
 */
public class TrustStoreValidationTests
{
    @Test()
    public void testNullSecrets()
    {
        TrustStoreValidation validation = new TrustStoreValidation(null);

        Assertions.assertThrows(RuntimeException.class, validation::perform);
    }

    @Test()
    public void testUnconfiguredTrustStore()
    {
        SecretsProvider secrets = TestSecretsProvider.unconfigured();
        TrustStoreValidation validation = new TrustStoreValidation(secrets);

        Assertions.assertDoesNotThrow(validation::perform);  // TrustStore is optional
    }

    @Test()
    public void testMissingTrustStore()
    {
        SecretsProvider secrets = TestSecretsProvider.forTrustStore("PKCS12", "keystore-missing.p12", "qwerty");
        TrustStoreValidation validation = new TrustStoreValidation(secrets);

        Assertions.assertThrows(RuntimeException.class, validation::perform);
    }

    @Test()
    public void testMalformedTrustStore()
    {
        SecretsProvider secrets = TestSecretsProvider.forTrustStore("PKCS12", "keystore-malformed.p12", "qwerty");
        TrustStoreValidation validation = new TrustStoreValidation(secrets);

        Assertions.assertThrows(RuntimeException.class, validation::perform);
    }

    @Test()
    public void testEmptyTrustStore()
    {
        SecretsProvider secrets = TestSecretsProvider.forTrustStore("PKCS12", "keystore-empty.p12", "qwerty");
        TrustStoreValidation validation = new TrustStoreValidation(secrets);

        Assertions.assertThrows(RuntimeException.class, validation::perform);
    }

    @Test()
    public void testInvalidTrustStore()
    {
        SecretsProvider secrets = TestSecretsProvider.forTrustStore("PKCS12", "keystore-secret.p12", "qwerty");
        TrustStoreValidation validation = new TrustStoreValidation(secrets);

        Assertions.assertThrows(RuntimeException.class, validation::perform);
    }

    @Test()
    public void testValidTrustStore()
    {
        SecretsProvider secrets = TestSecretsProvider.forTrustStore("PKCS12", "keystore-certificate.p12", "qwerty");
        TrustStoreValidation validation = new TrustStoreValidation(secrets);

        Assertions.assertDoesNotThrow(validation::perform);
    }
}
