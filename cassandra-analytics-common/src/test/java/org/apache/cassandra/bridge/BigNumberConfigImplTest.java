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
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link BigNumberConfigImpl}
 */
public class BigNumberConfigImplTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void testJsonSerialization() throws IOException
    {
        Map<String, BigNumberConfigImpl> map = ImmutableMap.of("field1", BigNumberConfigImpl.of(10, 4, 38, 19),
                                                               "field2", BigNumberConfigImpl.of(10, 4, 38, 19));
        String json = MAPPER.writeValueAsString(map);
        Map<String, BigNumberConfigImpl> deserialized = BigNumberConfigImpl.build(json);
        assertEquals(map, deserialized);
    }
}
