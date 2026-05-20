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

package org.apache.cassandra.spark.data;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reflection-based guard tests pinning the structural fix for the {@code SSTableCache.INSTANCE}
 * leak chain through {@link S3CassandraDataLayer.S3SSTable}. They assert two contracts:
 *
 * <ol>
 *   <li>{@code S3SSTable} and its sibling {@code S3SSTableSource} are {@code static} nested classes,
 *       so cached SSTable keys never carry a synthetic {@code this$0} pointing back at their owning
 *       {@link S3CassandraDataLayer}.</li>
 *   <li>The {@code S3SSTableContext} threaded through both classes carries only the documented
 *       fields and no outer reference, defending against a future refactor accidentally re-embedding
 *       a layer reference.</li>
 * </ol>
 *
 * <p>Why no GC reachability test: {@link S3CassandraDataLayer}'s constructors register a Spark
 * shutdown hook that captures {@code this} (see {@code ShutdownHookManager.addShutdownHook} calls).
 * That hook independently retains every constructor-built layer for JVM lifetime, so a
 * {@link java.lang.ref.WeakReference} would never be reclaimed regardless of whether the cache leak
 * is fixed. The reflection assertions below prove the cache key chain cannot reach the layer, which
 * is the precise contract this PR establishes; reachability through other roots (shutdown hooks,
 * RDD closures, broadcasts) is out of scope for this fix.
 */
public class S3SSTableLeakTests
{
    private static final String SOURCE_CLASS_NAME = "org.apache.cassandra.spark.data.S3CassandraDataLayer$S3SSTableSource";
    private static final String CONTEXT_CLASS_NAME = "org.apache.cassandra.spark.data.S3CassandraDataLayer$S3SSTableContext";

    /**
     * Documented field set for {@code S3SSTableContext}. Names must match exactly the field names in
     * {@link S3CassandraDataLayer.S3SSTableContext}; if you legitimately add or rename a field there,
     * update this set in the same change so reviewers see both edits together.
     */
    private static final Set<String> EXPECTED_CONTEXT_FIELDS = new HashSet<>(Arrays.asList(
        "clusterName",
        "datacenter",
        "s3BackupReader",
        "dataChunkBufferSize",
        "dataMaxBufferSize",
        "sstableS3ReadTimeoutSeconds",
        "sstableDataPublisherReadEnabled",
        "stats"
    ));

    @Test
    public void testS3SSTableAndSourceAreStaticWithNoOuterReference() throws Exception
    {
        // S3SSTable is the cache key inside SSTableCache.INSTANCE. If it ever stops being static,
        // the synthetic this$0 field returns and the leak chain is back.
        Class<?> sstable = S3CassandraDataLayer.S3SSTable.class;
        assertThat(Modifier.isStatic(sstable.getModifiers()))
                .as("S3CassandraDataLayer.S3SSTable must be static so cached keys do not pin the layer")
                .isTrue();
        assertNoSyntheticOuterReference(sstable);

        // S3SSTableSource is held transitively via openInputStream() / newSourceForTesting(). It must
        // also be static, otherwise the streaming path re-introduces the same outer reference.
        Class<?> source = Class.forName(SOURCE_CLASS_NAME);
        assertThat(Modifier.isStatic(source.getModifiers()))
                .as("S3SSTableSource must be a static nested class (binary name %s)", SOURCE_CLASS_NAME)
                .isTrue();
        assertNoSyntheticOuterReference(source);
    }

    @Test
    public void testS3SSTableContextHasOnlyExpectedFieldsAndNoOuterReference() throws Exception
    {
        // S3SSTableContext is the package-private bundle threaded into S3SSTable / S3SSTableSource.
        // Class.forName works because this test lives in the same package as S3CassandraDataLayer.
        Class<?> context = Class.forName(CONTEXT_CLASS_NAME);
        assertThat(Modifier.isStatic(context.getModifiers()))
                .as("S3SSTableContext must be static; an inner class would re-introduce the leak via this$0")
                .isTrue();
        assertNoSyntheticOuterReference(context);

        // Compare only declared instance fields. We tolerate compiler-synthetic fields by skipping
        // any field whose isSynthetic() is true (the static-class check above already excludes
        // this$0 specifically; this is belt-and-suspenders for, e.g., switch-table synthetics).
        Set<String> actual = Arrays.stream(context.getDeclaredFields())
                                   .filter(f -> !f.isSynthetic())
                                   .filter(f -> !Modifier.isStatic(f.getModifiers()))
                                   .map(Field::getName)
                                   .collect(Collectors.toSet());
        assertThat(actual)
                .as("S3SSTableContext field set drifted; update EXPECTED_CONTEXT_FIELDS and review whether the "
                    + "added field carries a back-edge to S3CassandraDataLayer (it must not)")
                .isEqualTo(EXPECTED_CONTEXT_FIELDS);
    }

    /**
     * Walks the declared fields of {@code clazz} and fails if any field is named {@code this$0} (or
     * any other compiler-synthetic outer-reference style). Static nested classes never have such a
     * field, so this is the canonical proof that the leak chain is cut.
     */
    private static void assertNoSyntheticOuterReference(Class<?> clazz)
    {
        for (Field field : clazz.getDeclaredFields())
        {
            assertThat(field.getName().startsWith("this$"))
                    .as("%s unexpectedly declares synthetic outer-reference field %s; "
                        + "this means the class became a non-static inner class and the SSTableCache leak chain is back",
                        clazz.getName(), field.getName())
                    .isFalse();
        }
    }
}
