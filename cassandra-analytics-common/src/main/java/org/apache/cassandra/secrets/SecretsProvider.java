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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;

import org.jetbrains.annotations.NotNull;

/**
 * A Secrets provider class
 */
public interface SecretsProvider
{
    /**
     * Provides an initialization mechanism for the secrets after the factory has created the instance of
     * the object by passing the {@code environmentOptions}.
     *
     * @param environmentOptions the environment options
     */
    void initialize(@NotNull Map<String, String> environmentOptions);

    /**
     * @return true if the keystore secrets have been provided, false otherwise
     */
    boolean hasKeyStoreSecrets();

    /**
     * @return an input stream to the keystore source
     * @throws IOException when an IO exception occurs
     */
    InputStream keyStoreInputStream() throws IOException;

    /**
     * @return a character array for the keystore password
     */
    char[] keyStorePassword();

    /**
     * @return the keystore type
     */
    default String keyStoreType()
    {
        return "PKCS12";
    }

    /**
     * @return true if the truststore secrets have been provided, false otherwise
     */
    boolean hasTrustStoreSecrets();

    /**
     * @return an input stream to the truststore source
     * @throws IOException when an IO exception occurs
     */
    InputStream trustStoreInputStream() throws IOException;

    /**
     * @return a character array for the truststore password
     */
    char[] trustStorePassword();

    /**
     * @return the truststore type
     */
    default String trustStoreType()
    {
        return "JKS";
    }

    /**
     * Validates the mutual TLS secrets
     */
    void validateMutualTLS();

    /**
     * Returns a secret that is found under the {@code secretName}. This name would correspond for example
     * to a file name under the secrets directory, or to an environment variable that contains the secret name.
     *
     * @param secretName the name of the secret in the underlying secret configuration.
     * @return a secret that is found under the {@code secretName}
     */
    String secretByName(String secretName);

    /**
     * @return the path for the secrets directory
     */
    Path secretsPath();
}
