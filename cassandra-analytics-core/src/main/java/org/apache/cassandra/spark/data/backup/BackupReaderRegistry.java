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

package org.apache.cassandra.spark.data.backup;

import java.util.Collections;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Driver-side registry of {@link BackupReaderFactory} implementations keyed by a short string
 * type name. Drivers register their factory once at startup; the data layer and prebuild path
 * then look it up by the {@code backupReaderType} option. Executors receive the factory through
 * closure serialization and never consult this registry.
 *
 * <p>There is no default type. {@link #factoryFor(String)} and
 * {@link #create(String, BackupReaderConfig)} throw {@link IllegalArgumentException} listing the
 * currently-registered types when no match is found.
 */
public final class BackupReaderRegistry
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BackupReaderRegistry.class);

    // Skip-list map so registered-types listings (used in error messages) are alphabetized.
    private static final ConcurrentSkipListMap<String, BackupReaderFactory> FACTORIES = new ConcurrentSkipListMap<>();

    private BackupReaderRegistry()
    {
    }

    /**
     * Registers a factory for the given type. Re-registering the same type replaces the previous
     * factory (the most recent registration wins).
     */
    public static void register(String type, BackupReaderFactory factory)
    {
        if (type == null || type.isEmpty())
        {
            throw new IllegalArgumentException("backupReaderType cannot be null or empty");
        }
        if (factory == null)
        {
            throw new IllegalArgumentException("BackupReaderFactory cannot be null");
        }
        BackupReaderFactory previous = FACTORIES.put(type, factory);
        if (previous != null)
        {
            LOGGER.warn("BackupReaderRegistry: replacing existing factory for type={}", type);
        }
        else
        {
            LOGGER.info("BackupReaderRegistry: registered factory for type={}", type);
        }
    }

    /** Returns the factory registered for the given type, or throws if none is registered. */
    public static BackupReaderFactory factoryFor(String type)
    {
        BackupReaderFactory factory = FACTORIES.get(type);
        if (factory == null)
        {
            throw new IllegalArgumentException(formatMissingTypeMessage(type));
        }
        return factory;
    }

    /** Resolves the factory for {@code type} and invokes it with {@code config}. */
    public static BackupReader create(String type, BackupReaderConfig config)
    {
        BackupReader reader = factoryFor(type).create(config);
        LOGGER.info("BackupReaderRegistry: created backup reader of type {}", type);
        return reader;
    }

    /** Returns an unmodifiable view of the registered type names. */
    public static Set<String> registeredTypes()
    {
        return Collections.unmodifiableSet(new TreeMap<>(FACTORIES).keySet());
    }

    private static String formatMissingTypeMessage(String requestedType)
    {
        Set<String> known = registeredTypes();
        return String.format(
            "no backup reader registered for type '%s'; "
            + "call BackupReaderRegistry.register(...) at driver startup, "
            + "or set the 'backupReaderType' option to a registered type. "
            + "Registered types: %s",
            requestedType,
            known.isEmpty() ? "[]" : known.toString());
    }
}
