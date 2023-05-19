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

package org.apache.cassandra.bridge;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.spark.utils.MapUtils;

/**
 * An implementation of the {@link BigNumberConfig} that supports JDK, Kryo, as well as jackson serialization
 */
public class BigNumberConfigImpl implements BigNumberConfig, Serializable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BigNumberConfigImpl.class);

    private int bigIntegerPrecision;
    private int bigIntegerScale;
    private int bigDecimalPrecision;
    private int bigDecimalScale;

    @JsonCreator
    public BigNumberConfigImpl(@JsonProperty("bigIntegerPrecision") int bigIntegerPrecision,
                               @JsonProperty("bigIntegerScale")     int bigIntegerScale,
                               @JsonProperty("bigDecimalPrecision") int bigDecimalPrecision,
                               @JsonProperty("bigDecimalScale")     int bigDecimalScale)
    {
        this.bigIntegerPrecision = bigIntegerPrecision;
        this.bigIntegerScale     = bigIntegerScale;
        this.bigDecimalPrecision = bigDecimalPrecision;
        this.bigDecimalScale     = bigDecimalScale;
    }

    public static BigNumberConfigImpl of(int bigIntegerPrecision,
                                         int bigIntegerScale,
                                         int bigDecimalPrecision,
                                         int bigDecimalScale)
    {
        return new BigNumberConfigImpl(bigIntegerPrecision, bigIntegerScale, bigDecimalPrecision, bigDecimalScale);
    }

    public static Map<String, BigNumberConfigImpl> build(Map<String, String> options)
    {
        return Optional.ofNullable(options.get(MapUtils.lowerCaseKey("bigNumberConfig")))
                       .map(BigNumberConfigImpl::build)
                       .orElse(Collections.emptyMap());
    }

    public static Map<String, BigNumberConfigImpl> build(String bigNumberConfig)
    {
        TypeReference<HashMap<String, BigNumberConfigImpl>> typeRef =
            new TypeReference<HashMap<String, BigNumberConfigImpl>>() {};  // CHECKSTYLE IGNORE: Empty anonymous inner class
        try
        {
            return new ObjectMapper().readValue(bigNumberConfig, typeRef);
        }
        catch (IOException exception)
        {
            LOGGER.error("IOException could not read BigNumberConfig json", exception);
            throw new RuntimeException(exception);
        }
    }

    @JsonProperty
    public int bigIntegerPrecision()
    {
        return bigIntegerPrecision;
    }

    @JsonProperty
    public int bigIntegerScale()
    {
        return bigIntegerScale;
    }

    @JsonProperty
    public int bigDecimalPrecision()
    {
        return bigDecimalPrecision;
    }

    @JsonProperty
    public int bigDecimalScale()
    {
        return bigDecimalScale;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(),
                            bigIntegerPrecision,
                            bigIntegerScale,
                            bigDecimalPrecision,
                            bigDecimalScale);
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        if (other == null || this.getClass() != other.getClass())
        {
            return false;
        }

        BigNumberConfigImpl that = (BigNumberConfigImpl) other;
        return bigIntegerPrecision == that.bigIntegerPrecision
            && bigIntegerScale == that.bigIntegerScale
            && bigDecimalPrecision == that.bigDecimalPrecision
            && bigDecimalScale == that.bigDecimalScale;
    }

    // JDK Serialization

    private void readObject(ObjectInputStream in) throws IOException
    {
        bigIntegerPrecision = in.readInt();
        bigIntegerScale = in.readInt();
        bigDecimalPrecision = in.readInt();
        bigDecimalScale = in.readInt();
    }

    private void writeObject(ObjectOutputStream out) throws IOException
    {
        out.writeInt(bigIntegerPrecision);
        out.writeInt(bigIntegerScale);
        out.writeInt(bigDecimalPrecision);
        out.writeInt(bigDecimalScale);
    }

    // Kryo Serialization

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<BigNumberConfigImpl>
    {

        public void write(Kryo kryo, Output out, BigNumberConfigImpl bigNumberConfig)
        {
            out.writeInt(bigNumberConfig.bigIntegerPrecision);
            out.writeInt(bigNumberConfig.bigIntegerScale);
            out.writeInt(bigNumberConfig.bigDecimalPrecision);
            out.writeInt(bigNumberConfig.bigDecimalScale);
        }

        public BigNumberConfigImpl read(Kryo kryo, Input input, Class<BigNumberConfigImpl> type)
        {
            return BigNumberConfigImpl.of(input.readInt(), input.readInt(), input.readInt(), input.readInt());
        }
    }
}
