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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.utils.MapUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Encapsulates SSL configuration for Sidecar
 */
public class SslConfig implements Serializable
{
    private static final long serialVersionUID = -3844712192096436932L;
    private static final Logger LOGGER = LoggerFactory.getLogger(SslConfig.class);
    public static final String SECRETS_PATH = "SECRETS_PATH";
    public static final String KEYSTORE_PATH = "KEYSTORE_PATH";
    public static final String KEYSTORE_BASE64_ENCODED = "KEYSTORE_BASE64_ENCODED";
    public static final String KEYSTORE_PASSWORD = "KEYSTORE_PASSWORD";
    public static final String KEYSTORE_TYPE = "KEYSTORE_TYPE";
    public static final String DEFAULT_KEYSTORE_TYPE = "PKCS12";
    public static final String TRUSTSTORE_PATH = "TRUSTSTORE_PATH";
    public static final String TRUSTSTORE_BASE64_ENCODED = "TRUSTSTORE_BASE64_ENCODED";
    public static final String TRUSTSTORE_PASSWORD = "TRUSTSTORE_PASSWORD";
    public static final String TRUSTSTORE_TYPE = "TRUSTSTORE_TYPE";
    public static final String DEFAULT_TRUSTSTORE_TYPE = "JKS";

    protected String secretsPath;
    protected String keyStorePath;
    protected String base64EncodedKeyStore;
    protected String keyStorePassword;
    protected String keyStoreType;
    protected String trustStorePath;
    protected String base64EncodedTrustStore;
    protected String trustStorePassword;
    protected String trustStoreType;

    protected SslConfig(Builder<?> builder)
    {
        secretsPath = builder.secretsPath;
        keyStorePath = builder.keyStorePath;
        base64EncodedKeyStore = builder.base64EncodedKeyStore;
        keyStorePassword = builder.keyStorePassword;
        keyStoreType = builder.keyStoreType;
        trustStorePath = builder.trustStorePath;
        base64EncodedTrustStore = builder.base64EncodedTrustStore;
        trustStorePassword = builder.trustStorePassword;
        trustStoreType = builder.trustStoreType;
    }

    /**
     * @return the path to the secrets directory
     */
    public String secretsPath()
    {
        return secretsPath;
    }

    /**
     * @return the path to the key store file
     */
    public String keyStorePath()
    {
        return keyStorePath;
    }

    /**
     * @return the encoded key store
     */
    public String base64EncodedKeyStore()
    {
        return base64EncodedKeyStore;
    }

    /**
     * @return the key store password
     */
    public String keyStorePassword()
    {
        return keyStorePassword;
    }

    /**
     * @return the key store type
     */
    public String keyStoreType()
    {
        return keyStoreType != null ? keyStoreType : DEFAULT_KEYSTORE_TYPE;
    }

    /**
     * @return the path to the trust store
     */
    public String trustStorePath()
    {
        return trustStorePath;
    }

    /**
     * @return the encoded trust store
     */
    public String base64EncodedTrustStore()
    {
        return base64EncodedTrustStore;
    }

    /**
     * @return the trust store password
     */
    public String trustStorePassword()
    {
        return trustStorePassword;
    }

    /**
     * @return the trust store type
     */
    public String trustStoreType()
    {
        return trustStoreType != null ? trustStoreType : DEFAULT_TRUSTSTORE_TYPE;
    }

    // JDK Serialization

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        LOGGER.debug("Falling back to JDK deserialization");
        this.secretsPath = readNullableString(in);
        this.keyStorePath = readNullableString(in);
        this.base64EncodedKeyStore = readNullableString(in);
        this.keyStorePassword = readNullableString(in);
        this.keyStoreType = readNullableString(in);
        this.trustStorePath = readNullableString(in);
        this.base64EncodedTrustStore = readNullableString(in);
        this.trustStorePassword = readNullableString(in);
        this.trustStoreType = readNullableString(in);
    }

    private void writeObject(ObjectOutputStream out) throws IOException
    {
        LOGGER.debug("Falling back to JDK serialization");
        writeNullableString(out, secretsPath);
        writeNullableString(out, keyStorePath);
        writeNullableString(out, base64EncodedKeyStore);
        writeNullableString(out, keyStorePassword);
        writeNullableString(out, keyStoreType);
        writeNullableString(out, trustStorePath);
        writeNullableString(out, base64EncodedTrustStore);
        writeNullableString(out, trustStorePassword);
        writeNullableString(out, trustStoreType);
    }

    private String readNullableString(ObjectInputStream in) throws IOException
    {
        return in.readBoolean() ? in.readUTF() : null;
    }

    private void writeNullableString(ObjectOutputStream out, String string) throws IOException
    {
        if (string != null)
        {
            out.writeBoolean(true);
            out.writeUTF(string);
        }
        else
        {
            out.writeBoolean(false);
        }
    }

    // Kryo

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<SslConfig>
    {
        public SslConfig read(Kryo kryo, Input in, Class type)
        {
            return new Builder<>()
                   .secretsPath(in.readString())
                   .keyStorePath(in.readString())
                   .base64EncodedKeyStore(in.readString())
                   .keyStorePassword(in.readString())
                   .keyStoreType(in.readString())
                   .trustStorePath(in.readString())
                   .base64EncodedTrustStore(in.readString())
                   .trustStorePassword(in.readString())
                   .trustStoreType(in.readString())
                   .build();
        }

        public void write(Kryo kryo, Output out, SslConfig config)
        {
            out.writeString(config.secretsPath);
            out.writeString(config.keyStorePath);
            out.writeString(config.base64EncodedKeyStore);
            out.writeString(config.keyStorePassword);
            out.writeString(config.keyStoreType);
            out.writeString(config.trustStorePath);
            out.writeString(config.base64EncodedTrustStore);
            out.writeString(config.trustStorePassword);
            out.writeString(config.trustStoreType);
        }
    }

    @Nullable
    public static SslConfig create(Map<String, String> options)
    {
        String secretsPath = MapUtils.getOrDefault(options, SECRETS_PATH, null);
        String keyStorePath = MapUtils.getOrDefault(options, KEYSTORE_PATH, null);
        String encodedKeyStore = MapUtils.getOrDefault(options, KEYSTORE_BASE64_ENCODED, null);
        String keyStorePassword = MapUtils.getOrDefault(options, KEYSTORE_PASSWORD, null);
        String keyStoreType = MapUtils.getOrDefault(options, KEYSTORE_TYPE, null);
        String trustStorePath = MapUtils.getOrDefault(options, TRUSTSTORE_PATH, null);
        String encodedTrustStore = MapUtils.getOrDefault(options, TRUSTSTORE_BASE64_ENCODED, null);
        String trustStorePassword = MapUtils.getOrDefault(options, TRUSTSTORE_PASSWORD, null);
        String trustStoreType = MapUtils.getOrDefault(options, TRUSTSTORE_TYPE, null);

        // If any of the values are provided we try to create a valid SecretsConfig object
        if (secretsPath != null
            || keyStorePath != null
            || encodedKeyStore != null
            || keyStorePassword != null
            || keyStoreType != null
            || trustStorePath != null
            || encodedTrustStore != null
            || trustStorePassword != null
            || trustStoreType != null)
        {
            Builder<?> validatedConfig = new Builder<>()
                                         .secretsPath(secretsPath)
                                         .keyStorePath(keyStorePath)
                                         .base64EncodedKeyStore(encodedKeyStore)
                                         .keyStorePassword(keyStorePassword)
                                         .keyStoreType(keyStoreType)
                                         .trustStorePath(trustStorePath)
                                         .base64EncodedTrustStore(encodedTrustStore)
                                         .trustStorePassword(trustStorePassword)
                                         .trustStoreType(trustStoreType)
                                         .validate();
            LOGGER.info("Valid SSL configuration");
            return validatedConfig.build();
        }

        LOGGER.warn("No SSL configured");
        return null;
    }

    /**
     * {@code SslConfig} builder static inner class
     *
     * @param <T> itself
     */
    public static class Builder<T extends SslConfig.Builder<T>>
    {
        protected String secretsPath;
        protected String keyStorePath;
        protected String base64EncodedKeyStore;
        protected String keyStorePassword;
        protected String keyStoreType;
        protected String trustStorePath;
        protected String base64EncodedTrustStore;
        protected String trustStorePassword;
        protected String trustStoreType;

        /**
         * @return a reference to itself
         */
        @SuppressWarnings("unchecked")
        protected T self()
        {
            return (T) this;
        }

        /**
         * Sets the {@code secretsPath} and returns a reference to this Builder enabling method chaining
         *
         * @param secretsPath the {@code secretsPath} to set
         * @return a reference to this Builder
         */
        public T secretsPath(String secretsPath)
        {
            this.secretsPath = secretsPath;
            return self();
        }

        /**
         * Sets the {@code keyStorePath} and returns a reference to this Builder enabling method chaining
         *
         * @param keyStorePath the {@code keyStorePath} to set
         * @return a reference to this Builder
         */
        public T keyStorePath(String keyStorePath)
        {
            this.keyStorePath = keyStorePath;
            return self();
        }

        /**
         * Sets the {@code base64EncodedKeyStore} and returns a reference to this Builder enabling method chaining
         *
         * @param base64EncodedKeyStore the {@code base64EncodedKeyStore} to set
         * @return a reference to this Builder
         */
        public T base64EncodedKeyStore(String base64EncodedKeyStore)
        {
            this.base64EncodedKeyStore = base64EncodedKeyStore;
            return self();
        }

        /**
         * Sets the {@code keyStorePassword} and returns a reference to this Builder enabling method chaining
         *
         * @param keyStorePassword the {@code keyStorePassword} to set
         * @return a reference to this Builder
         */
        public T keyStorePassword(String keyStorePassword)
        {
            this.keyStorePassword = keyStorePassword;
            return self();
        }

        /**
         * Sets the {@code keyStoreType} and returns a reference to this Builder enabling method chaining
         *
         * @param keyStoreType the {@code keyStoreType} to set
         * @return a reference to this Builder
         */
        public T keyStoreType(String keyStoreType)
        {
            this.keyStoreType = keyStoreType;
            return self();
        }

        /**
         * Sets the {@code trustStorePath} and returns a reference to this Builder enabling method chaining
         *
         * @param trustStorePath the {@code trustStorePath} to set
         * @return a reference to this Builder
         */
        public T trustStorePath(String trustStorePath)
        {
            this.trustStorePath = trustStorePath;
            return self();
        }

        /**
         * Sets the {@code base64EncodedTrustStore} and returns a reference to this Builder enabling method chaining
         *
         * @param base64EncodedTrustStore the {@code base64EncodedTrustStore} to set
         * @return a reference to this Builder
         */
        public T base64EncodedTrustStore(String base64EncodedTrustStore)
        {
            this.base64EncodedTrustStore = base64EncodedTrustStore;
            return self();
        }

        /**
         * Sets the {@code trustStorePassword} and returns a reference to this Builder enabling method chaining
         *
         * @param trustStorePassword the {@code trustStorePassword} to set
         * @return a reference to this Builder
         */
        public T trustStorePassword(String trustStorePassword)
        {
            this.trustStorePassword = trustStorePassword;
            return self();
        }

        /**
         * Sets the {@code trustStoreType} and returns a reference to this Builder enabling method chaining
         *
         * @param trustStoreType the {@code trustStoreType} to set
         * @return a reference to this Builder
         */
        public T trustStoreType(String trustStoreType)
        {
            this.trustStoreType = trustStoreType;
            return self();
        }

        /**
         * Returns a {@code SslConfig} built from the parameters previously set
         *
         * @return a {@code SslConfig} built with parameters of this {@code SslConfig.Builder}
         */
        public SslConfig build()
        {
            return new SslConfig(this);
        }

        protected T validate()
        {
            if (keyStorePath != null && base64EncodedKeyStore != null)
            {
                throw new IllegalArgumentException(
                        String.format("Both '%s' and '%s' options were provided. Only one of the options can be provided",
                                      KEYSTORE_PATH, KEYSTORE_BASE64_ENCODED));
            }

            if (keyStorePassword != null)
            {
                // When the keystore password is provided, either the key store path or
                // the encoded key store must be provided
                if (keyStorePath == null && base64EncodedKeyStore == null)
                {
                    throw new IllegalArgumentException(
                            String.format("One of the '%s' or '%s' options must be provided when the '%s' option is provided",
                                          KEYSTORE_PATH, KEYSTORE_BASE64_ENCODED, KEYSTORE_PASSWORD));
                }
            }
            else
            {

                throw new IllegalArgumentException(
                        String.format("The '%s' option must be provided when either the '%s' or '%s' options are provided",
                                      KEYSTORE_PASSWORD, KEYSTORE_PATH, KEYSTORE_BASE64_ENCODED));
            }

            if (trustStorePassword != null)
            {
                // Only one of trust store path or encoded trust store must be provided, not both
                if (trustStorePath != null && base64EncodedTrustStore != null)
                {
                    throw new IllegalArgumentException(
                            String.format("Both '%s' and '%s' options were provided. Only one of the options can be provided",
                                          TRUSTSTORE_PATH, TRUSTSTORE_BASE64_ENCODED));
                }

                // When the trust store password is provided, either the trust store
                // path or the encoded trust store must be provided
                if (trustStorePath == null && base64EncodedTrustStore == null)
                {
                    throw new IllegalArgumentException(
                           String.format("One of the '%s' or '%s' options must be provided when the '%s' option is provided",
                                          TRUSTSTORE_PATH, TRUSTSTORE_BASE64_ENCODED, TRUSTSTORE_PASSWORD));
                }
            }
            else if (trustStorePath != null || base64EncodedTrustStore != null)
            {
                throw new IllegalArgumentException(
                        String.format("The '%s' option must be provided when either the '%s' or '%s' options are provided",
                                      TRUSTSTORE_PASSWORD, TRUSTSTORE_PATH, TRUSTSTORE_BASE64_ENCODED));
            }
            return self();
        }
    }
}
