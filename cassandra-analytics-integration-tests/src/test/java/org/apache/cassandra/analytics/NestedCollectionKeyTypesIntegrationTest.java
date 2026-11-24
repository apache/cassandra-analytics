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

package org.apache.cassandra.analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.vdurmont.semver4j.Semver;

import org.apache.cassandra.bridge.CassandraVersion;

/**
 * Bulk writes partition and clustering keys that nest sets and maps in frozen UDTs, tuples, collections and vectors,
 * and keys of descending clustering columns.
 * <p>
 * Cassandra sorts the elements of a set and the entries of a map that a CQL literal holds at any depth, but, depending
 * on the version, not those nested in a bound value. Rows whose keys the writer did not sort the same way would be
 * stored under other keys, missing from scans by token and from lookups by the CQL literals. The literals list the
 * elements out of order.
 */
class NestedCollectionKeyTypesIntegrationTest extends KeyTypesIntegrationTestBase
{
    @Override
    protected List<String> userDefinedTypes()
    {
        List<String> userDefinedTypes = new ArrayList<>(super.userDefinedTypes());
        userDefinedTypes.add("tagged_udt (id int, tags set<text>)");
        userDefinedTypes.add("attributed_udt (id int, attrs map<text, int>)");
        userDefinedTypes.add("wrapper_udt (inner frozen<tagged_udt>)");
        return userDefinedTypes;
    }

    @Override
    List<KeyType> keyTypes()
    {
        List<KeyType> keyTypes = new ArrayList<>(Arrays.asList(
        new KeyType("set_in_udt", "frozen<tagged_udt>", i -> String.format("{id: %d, tags: {'p', 'a', 'banana_%d'}}", i, i)),
        new KeyType("map_in_udt", "frozen<attributed_udt>",
                    i -> String.format("{id: %d, attrs: {'p': 2, 'a': 1, 'banana_%d': %d}}", i, i, i)),
        new KeyType("udt_in_udt", "frozen<wrapper_udt>", i -> String.format("{inner: {id: %d, tags: {'p', 'a'}}}", i)),
        new KeyType("set_in_tuple", "frozen<tuple<int, frozen<set<text>>>>", i -> String.format("(%d, {'p', 'a'})", i)),
        new KeyType("set_in_list", "frozen<list<frozen<set<text>>>>", i -> String.format("[{'p', 'a', 'banana_%d'}]", i)),
        new KeyType("set_in_set", "frozen<set<frozen<set<text>>>>", i -> String.format("{{'p', 'a'}, {'banana_%d'}}", i)),
        new KeyType("map_keyed_by_set", "frozen<map<frozen<set<text>>, int>>",
                    i -> String.format("{{'p', 'a', 'banana_%d'}: %d}", i, i)),
        new KeyType("set_of_udts_with_sets", "frozen<set<frozen<tagged_udt>>>",
                    i -> String.format("{{id: %d, tags: {'p', 'a'}}, {id: -1, tags: {'z', 'b'}}}", i))));
        if (acceptsUdtLiteralsInDescendingOrder())
        {
            keyTypes.add(KeyType.descending("set_in_udt_descending", "frozen<tagged_udt>",
                                            i -> String.format("{id: %d, tags: {'p', 'a', 'banana_%d'}}", i, i)));
            // From Cassandra 5.0 on, the SSTable writer cannot serialize a UDT value for a descending clustering column
            keyTypes.add(KeyType.descending("udt_descending", "frozen<simple_udt>", i -> String.format("{a: %d, b: 'udt'}", i)));
        }
        if (cassandraVersion().versionNumber() >= CassandraVersion.FIVEZERO.versionNumber())
        {
            keyTypes.add(new KeyType("set_in_vector", "vector<frozen<set<text>>, 2>",
                                     i -> String.format("[{'p', 'a', 'banana_%d'}, {'z', 'b'}]", i)));
        }
        return keyTypes;
    }

    /**
     * @return whether the cluster takes UDT literals for a clustering column in descending order, which Cassandra 4.1
     * only does from 4.1.8 on (CASSANDRA-20050)
     */
    private boolean acceptsUdtLiteralsInDescendingOrder()
    {
        Semver version = new Semver(cluster.getFirstRunningInstance().getReleaseVersionString(), Semver.SemverType.LOOSE);
        return !(version.getMajor() == 4 && version.getMinor() == 1 && version.getPatch() < 8);
    }
}
