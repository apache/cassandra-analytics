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

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

import static org.apache.cassandra.secrets.SslConfig.KEYSTORE_BASE64_ENCODED;
import static org.apache.cassandra.secrets.SslConfig.KEYSTORE_PASSWORD;
import static org.apache.cassandra.secrets.SslConfig.KEYSTORE_PATH;
import static org.apache.cassandra.secrets.SslConfig.TRUSTSTORE_BASE64_ENCODED;
import static org.apache.cassandra.secrets.SslConfig.TRUSTSTORE_PASSWORD;
import static org.apache.cassandra.secrets.SslConfig.TRUSTSTORE_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Unit tests for the {@link SslConfig} class
 */
public class SslConfigTest
{
    @Test
    public void testEmptyConfiguration()
    {
        assertThat(SslConfig.create(Collections.emptyMap())).isNull();
    }

    @Test
    public void validationFailsWhenBothKeyStorePathAndEncodedKeyStoreAreProvided()
    {
        try
        {
            new SslConfig.Builder<>()
            .keyStorePath("/foo")
            .base64EncodedKeyStore("a")
            .validate();
            fail("validation should fail when both key store path and encoded key store are provided");
        }
        catch (IllegalArgumentException exception)
        {
            assertThat(exception.getMessage()).isEqualTo("Both 'KEYSTORE_PATH' and 'KEYSTORE_BASE64_ENCODED' options were provided. "
                       + "Only one of the options can be provided");
        }
    }

    @Test
    public void validationFailsWhenKeystorePasswordIsNotProvided1()
    {
        try
        {
            new SslConfig.Builder<>()
            .keyStorePath("/foo")
            .validate();
            fail("validation should fail the keystore path is provided and the keystore password is not provided");
        }
        catch (IllegalArgumentException exception)
        {
            assertThat(exception.getMessage()).isEqualTo("The 'KEYSTORE_PASSWORD' option must be provided when either the 'KEYSTORE_PATH'"
                       + " or 'KEYSTORE_BASE64_ENCODED' options are provided");
        }
    }

    @Test
    public void validationFailsWhenKeystorePasswordIsNotProvided2()
    {
        try
        {
            new SslConfig.Builder<>()
            .base64EncodedKeyStore("a")
            .validate();
            fail("validation should fail when the encoded keystore is provided and the keystore password is not provided");
        }
        catch (IllegalArgumentException exception)
        {
            assertThat(exception.getMessage()).isEqualTo("The 'KEYSTORE_PASSWORD' option must be provided when either the 'KEYSTORE_PATH'"
                       + " or 'KEYSTORE_BASE64_ENCODED' options are provided");
        }
    }

    @Test
    public void validationFailsWhenBothTrustStorePathAndEncodedTrustStoreAreProvided()
    {
        try
        {
            new SslConfig.Builder<>()
            .keyStorePath("/foo")
            .keyStorePassword("pass")
            .trustStorePath("/bar")
            .base64EncodedTrustStore("a")
            .trustStorePassword("pass")
            .validate();
            fail("validation should fail when both trust store path and encoded trust store are provided");
        }
        catch (IllegalArgumentException exception)
        {
            assertThat(exception.getMessage()).isEqualTo("Both 'TRUSTSTORE_PATH' and 'TRUSTSTORE_BASE64_ENCODED' options were provided. "
                       + "Only one of the options can be provided");
        }
    }

    @Test
    public void validationFailsTrustStorePasswordIsNotProvided1()
    {
        try
        {
            new SslConfig.Builder<>()
            .keyStorePath("/foo")
            .keyStorePassword("pass")
            .trustStorePath("/bar")
            .validate();
            fail("validation should fail when trust store path is provided and trust store password is not provided");
        }
        catch (IllegalArgumentException exception)
        {
            assertThat(exception.getMessage()).isEqualTo("The 'TRUSTSTORE_PASSWORD' option must be provided when either the 'TRUSTSTORE_PATH'"
                       + " or 'TRUSTSTORE_BASE64_ENCODED' options are provided");
        }
    }

    @Test
    public void validationFailsTrustStorePasswordIsNotProvided2()
    {
        try
        {
            new SslConfig.Builder<>()
            .keyStorePath("/foo")
            .keyStorePassword("pass")
            .base64EncodedTrustStore("a")
            .validate();
            fail("validation should fail when encoded trust store is provided and trust store password is not provided");
        }
        catch (IllegalArgumentException exception)
        {
            assertThat(exception.getMessage()).isEqualTo("The 'TRUSTSTORE_PASSWORD' option must be provided when either the 'TRUSTSTORE_PATH'"
                       + " or 'TRUSTSTORE_BASE64_ENCODED' options are provided");
        }
    }

    @Test
    public void buildWithKeyStorePathAndPassword()
    {
        Map<String, String> options = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        options.put(KEYSTORE_PATH, "/foo");
        options.put(KEYSTORE_PASSWORD, "pass");
        SslConfig config = SslConfig.create(options);
        assertThat(config).isNotNull();
        assertThat(config.keyStorePath()).isEqualTo("/foo");
        assertThat(config.keyStorePassword()).isEqualTo("pass");
    }

    @Test
    public void buildWithEncodedKeyStoreAndPassword()
    {
        Map<String, String> options = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        options.put(KEYSTORE_BASE64_ENCODED, "AA");
        options.put(KEYSTORE_PASSWORD, "pass");
        SslConfig config = SslConfig.create(options);
        assertThat(config).isNotNull();
        assertThat(config.base64EncodedKeyStore()).isEqualTo("AA");
        assertThat(config.keyStorePassword()).isEqualTo("pass");
    }

    @Test
    public void buildWithTrustStorePathAndPassword()
    {
        Map<String, String> options = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        options.put(KEYSTORE_PATH, "/foo");
        options.put(KEYSTORE_PASSWORD, "pass");
        options.put(TRUSTSTORE_PATH, "/bar");
        options.put(TRUSTSTORE_PASSWORD, "passs");
        SslConfig config = SslConfig.create(options);
        assertThat(config).isNotNull();
        assertThat(config.keyStorePath()).isEqualTo("/foo");
        assertThat(config.keyStorePassword()).isEqualTo("pass");
        assertThat(config.trustStorePath()).isEqualTo("/bar");
        assertThat(config.trustStorePassword()).isEqualTo("passs");
    }

    @Test
    public void buildWithEncodedTrustStoreAndPassword()
    {
        Map<String, String> options = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        options.put(KEYSTORE_PATH, "/foo");
        options.put(KEYSTORE_PASSWORD, "pass");
        options.put(TRUSTSTORE_BASE64_ENCODED, "AAA");
        options.put(TRUSTSTORE_PASSWORD, "passs");
        SslConfig config = SslConfig.create(options);
        assertThat(config).isNotNull();
        assertThat(config.keyStorePath()).isEqualTo("/foo");
        assertThat(config.keyStorePassword()).isEqualTo("pass");
        assertThat(config.base64EncodedTrustStore()).isEqualTo("AAA");
        assertThat(config.trustStorePassword()).isEqualTo("passs");
    }
}
