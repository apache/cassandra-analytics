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

package org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.apache.cassandra.spark.common.SidecarInstanceFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toSet;

/**
 * Data class containing the configurations required for coordinated write.
 * The serialization format is JSON string. The class takes care of serialization and deserialization.
 */
public class CoordinatedWriteConf
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatedWriteConf.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    // The runtime type of ClusterConfProvider is erased; use clustersOf method to read the desired type back
    private final Map<String, ClusterConf> clusters;
    private final String rawJson;

    /**
     * Parse JSON string and create a CoordinatedWriteConf object with the specified ClusterConfProvider format
     *
     * @param json JSON string
     * @param clusterConfType concrete type of ClusterConfProvider that can be used for JSON serialization and deserialization
     * @return CoordinatedWriteConf object
     * @param <T> subtype of ClusterConfProvider
     */
    public static <T extends ClusterConf>
    CoordinatedWriteConf create(String json, ConsistencyLevel.CL consistencyLevel, Class<T> clusterConfType)
    {
        JavaType javaType = TypeFactory.defaultInstance().constructMapType(Map.class, String.class, clusterConfType);
        CoordinatedWriteConf result;
        try
        {
            result = new CoordinatedWriteConf(json, OBJECT_MAPPER.readValue(json, javaType));
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Unable to parse json string into CoordinatedWriteConf of " + clusterConfType.getSimpleName() +
                                               " due to " + e.getMessage(), e);
        }
        result.clusters().forEach((clusterId, cluster) -> {
            if (consistencyLevel.isLocal())
            {
                Preconditions.checkState(!StringUtils.isEmpty(cluster.localDc()),
                                         "localDc is not configured for cluster: " + clusterId + " for consistency level: " + consistencyLevel);
            }
            else
            {
                if (cluster.localDc() != null)
                {
                    LOGGER.warn("Ignoring the localDc configured for cluster, when consistency level is non-local. cluster={} consistencyLevel={}",
                                clusterId, consistencyLevel);
                }
            }
        });
        return result;
    }

    public CoordinatedWriteConf(Map<String, ? extends ClusterConf> clusters)
    {
        this.clusters = Collections.unmodifiableMap(clusters);
        try
        {
            this.rawJson = OBJECT_MAPPER.writeValueAsString(clusters);
        }
        catch (JsonProcessingException e)
        {
            throw new IllegalArgumentException("Input cannot be written as json string", e);
        }
    }

    public CoordinatedWriteConf(String rawJson, Map<String, ? extends ClusterConf> clusters)
    {
        this.rawJson = rawJson;
        this.clusters = Collections.unmodifiableMap(clusters);
    }

    public Map<String, ClusterConf> clusters()
    {
        return clusters;
    }

    @NotNull
    public ClusterConf cluster(String clusterId)
    {
        ClusterConf cluster = clusters.get(clusterId);
        if (cluster == null)
        {
            throw new NoSuchElementException("No ClusterConf is found for clusterId: " + clusterId);
        }
        return cluster;
    }

    public <T extends ClusterConf> Map<String, T> clustersOf(Class<T> clusterConfType)
    {
        // verify that map type can cast; there are only limited number of values and check is cheap
        clusters.values().forEach(v -> Preconditions.checkState(clusterConfType.isInstance(v),
                                                                "ClusterConfProvider value is not instance of " + clusterConfType));
        return (Map<String, T>) clusters;
    }

    public String toJson()
    {
        return rawJson;
    }

    @Override
    public String toString()
    {
        return "CoordinatedWriteConf{" +
               "json=" + rawJson +
               '}';
    }

    public interface ClusterConf
    {
        Set<SidecarInstance> sidecarContactPoints();

        @Nullable
        String localDc();

        @Nullable
        default String resolveLocalDc(ConsistencyLevel cl)
        {
            String localDc = localDc();
            boolean hasLocalDc = !StringUtils.isEmpty(localDc());
            if (!cl.isLocal() && hasLocalDc)
            {
                return null;
            }
            if (cl.isLocal() && !hasLocalDc)
            {
                throw new IllegalStateException("No localDc is specified for local consistency level: " + cl);
            }
            return localDc;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SimpleClusterConf implements ClusterConf
    {
        private final List<String> sidecarContactPointsValue;
        private final Set<SidecarInstance> sidecarContactPoints;
        private final @Nullable String localDc;

        @JsonCreator
        public SimpleClusterConf(@JsonProperty("sidecarContactPoints") List<String> sidecarContactPointsValue,
                                 @JsonProperty("localDc") String localDc)
        {
            this.sidecarContactPointsValue = sidecarContactPointsValue;
            this.sidecarContactPoints = buildSidecarContactPoints(sidecarContactPointsValue);
            this.localDc = localDc;
        }

        @JsonProperty("sidecarContactPoints")
        public List<String> sidecarContactPointsValue()
        {
            return sidecarContactPointsValue;
        }

        @Nullable
        @Override
        @JsonProperty("localDc")
        public String localDc()
        {
            return localDc;
        }

        @Override
        public Set<SidecarInstance> sidecarContactPoints()
        {
            return sidecarContactPoints;
        }

        private Set<SidecarInstance> buildSidecarContactPoints(List<String> sidecarContactPoints)
        {
            return sidecarContactPoints.stream()
                                       .filter(StringUtils::isNotEmpty)
                                       .map(SidecarInstanceFactory::createFromString)
                                       .collect(collectingAndThen(toSet(), Collections::unmodifiableSet));
        }
    }
}
