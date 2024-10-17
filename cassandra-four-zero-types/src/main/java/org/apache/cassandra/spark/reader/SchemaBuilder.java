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

package org.apache.cassandra.spark.reader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.bridge.CassandraSchema;
import org.apache.cassandra.bridge.CassandraTypesImplementation;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.statements.schema.CreateTypeStatement;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.spark.data.CassandraTypes;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.complex.CqlFrozen;
import org.apache.cassandra.spark.data.complex.CqlUdt;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.utils.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SchemaBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaBuilder.class);

    private final TableMetadata metadata;
    private final KeyspaceMetadata keyspaceMetadata;
    private final String createStmt;
    private final String keyspace;
    private final ReplicationFactor replicationFactor;
    private final CassandraTypes cassandraTypes;
    private final int indexCount;

    public SchemaBuilder(CqlTable table, Partitioner partitioner, boolean enableCdc)
    {
        this(table, partitioner, null, enableCdc);
    }

    public SchemaBuilder(CqlTable table, Partitioner partitioner)
    {
        this(table, partitioner, null, false);
    }

    public SchemaBuilder(CqlTable table, Partitioner partitioner, UUID tableId, boolean enableCdc)
    {
        this(table.createStatement(),
             table.keyspace(),
             table.replicationFactor(),
             partitioner,
             table::udtCreateStmts,
             tableId,
             0,
             enableCdc);
    }

    @VisibleForTesting
    public SchemaBuilder(String createStmt, String keyspace, ReplicationFactor replicationFactor)
    {
        this(createStmt, keyspace, replicationFactor, Partitioner.Murmur3Partitioner, bridge -> Collections.emptySet(), null, 0, false);
    }

    @VisibleForTesting
    public SchemaBuilder(String createStmt,
                         String keyspace,
                         ReplicationFactor replicationFactor,
                         Partitioner partitioner)
    {
        this(createStmt, keyspace, replicationFactor, partitioner, bridge -> Collections.emptySet(), null, 0, false);
    }

    public SchemaBuilder(String createStmt,
                         String keyspace,
                         ReplicationFactor replicationFactor,
                         Partitioner partitioner,
                         Function<CassandraTypes, Set<String>> udtStatementsProvider,
                         @Nullable UUID tableId,
                         int indexCount,
                         boolean enableCdc)
    {
        this.createStmt = createStmt;
        this.keyspace = keyspace;
        this.replicationFactor = replicationFactor;
        this.cassandraTypes = new CassandraTypesImplementation();
        this.indexCount = indexCount;

        Pair<KeyspaceMetadata, TableMetadata> updated = CassandraSchema.apply(schema ->
                updateSchema(schema,
                             this.keyspace,
                             udtStatementsProvider.apply(cassandraTypes),
                             this.createStmt,
                             partitioner,
                             this.replicationFactor,
                             tableId, enableCdc,
                             this::validateColumnMetaData));
        this.keyspaceMetadata = updated.left;
        this.metadata = updated.right;
    }

    // Update schema with the given keyspace, table and udt.
    // It creates the corresponding metadata and opens instances for keyspace and table, if needed.
    // At the end, it validates that the input keyspace and table both should have metadata exist and instance opened.
    private static Pair<KeyspaceMetadata, TableMetadata> updateSchema(Schema schema,
                                                                      String keyspace,
                                                                      Set<String> udtStatements,
                                                                      String createStatement,
                                                                      Partitioner partitioner,
                                                                      ReplicationFactor replicationFactor,
                                                                      UUID tableId,
                                                                      boolean enableCdc,
                                                                      Consumer<ColumnMetadata> columnValidator)
    {
        // Set up and open keyspace if needed
        IPartitioner cassPartitioner = CassandraTypesImplementation.getPartitioner(partitioner);
        setupKeyspace(schema, keyspace, replicationFactor, cassPartitioner);

        // Set up and open table if needed, parse UDTs and include when parsing table schema
        List<CreateTypeStatement.Raw> typeStatements = new ArrayList<>(udtStatements.size());
        for (String udt : udtStatements)
        {
            try
            {
                typeStatements.add((CreateTypeStatement.Raw) CQLFragmentParser
                        .parseAnyUnhandled(CqlParser::query, udt));
            }
            catch (RecognitionException exception)
            {
                LOGGER.error("Failed to parse type expression '{}'", udt);
                throw new IllegalStateException(exception);
            }
        }
        Types.RawBuilder typesBuilder = Types.rawBuilder(keyspace);
        for (CreateTypeStatement.Raw st : typeStatements)
        {
            st.addToRawBuilder(typesBuilder);
        }
        Types types = typesBuilder.build();
        TableMetadata.Builder builder = CQLFragmentParser
                .parseAny(CqlParser::createTableStatement, createStatement, "CREATE TABLE")
                .keyspace(keyspace)
                .prepare(null)
                .builder(types)
                .partitioner(cassPartitioner);

        if (tableId != null)
        {
            builder.id(TableId.fromUUID(tableId));
        }

        TableMetadata tableMetadata = builder.build();

        if (tableMetadata.params.cdc != enableCdc)
        {
            tableMetadata = tableMetadata.unbuild()
                                         .params(tableMetadata.params.unbuild()
                                                                     .cdc(enableCdc)
                                                                     .build())
                                         .build();
        }

        tableMetadata.columns().forEach(columnValidator);
        setupTableAndUdt(schema, keyspace, tableMetadata, types);

        return validateKeyspaceTable(schema, keyspace, tableMetadata.name);
    }

    private void validateColumnMetaData(@NotNull ColumnMetadata column)
    {
        validateType(column.type);
    }

    private void validateType(AbstractType<?> type)
    {
        validateType(type.asCQL3Type());
    }

    private void validateType(CQL3Type cqlType)
    {
        if (!(cqlType instanceof CQL3Type.Native)
                && !(cqlType instanceof CQL3Type.Collection)
                && !(cqlType instanceof CQL3Type.UserDefined)
                && !(cqlType instanceof CQL3Type.Tuple))
        {
            throw new UnsupportedOperationException("Only native, collection, tuples or UDT data types are supported, "
                                                  + "unsupported data type: " + cqlType.toString());
        }

        if (cqlType instanceof CQL3Type.Native)
        {
            CqlField.CqlType type = cassandraTypes.parseType(cqlType.toString());
            if (!type.isSupported())
            {
                throw new UnsupportedOperationException(type.name() + " data type is not supported");
            }
        }
        else if (cqlType instanceof CQL3Type.Collection)
        {
            // Validate collection inner types
            CQL3Type.Collection collection = (CQL3Type.Collection) cqlType;
            CollectionType<?> type = (CollectionType<?>) collection.getType();
            switch (type.kind)
            {
                case LIST:
                    validateType(((ListType<?>) type).getElementsType());
                    return;
                case SET:
                    validateType(((SetType<?>) type).getElementsType());
                    return;
                case MAP:
                    validateType(((MapType<?, ?>) type).getKeysType());
                    validateType(((MapType<?, ?>) type).getValuesType());
                    return;
                default:
                    // Do nothing
            }
        }
        else if (cqlType instanceof CQL3Type.Tuple)
        {
            CQL3Type.Tuple tuple = (CQL3Type.Tuple) cqlType;
            TupleType tupleType = (TupleType) tuple.getType();
            for (AbstractType<?> subType : tupleType.allTypes())
            {
                validateType(subType);
            }
        }
        else
        {
            // Validate UDT inner types
            UserType userType = (UserType) ((CQL3Type.UserDefined) cqlType).getType();
            for (AbstractType<?> innerType : userType.fieldTypes())
            {
                validateType(innerType);
            }
        }
    }

    private static boolean keyspaceMetadataExists(Schema schema, String keyspaceName)
    {
        return schema.getKeyspaceMetadata(keyspaceName) != null;
    }

    private static boolean tableMetadataExists(Schema schema, String keyspaceName, String tableName)
    {
        KeyspaceMetadata ksMetadata = schema.getKeyspaceMetadata(keyspaceName);
        if (ksMetadata == null)
        {
            return false;
        }

        return ksMetadata.hasTable(tableName);
    }

    private static boolean keyspaceInstanceExists(Schema schema, String keyspaceName)
    {
        return schema.getKeyspaceInstance(keyspaceName) != null;
    }

    private static boolean tableInstanceExists(Schema schema, String keyspaceName, String tableName)
    {
        Keyspace keyspace = schema.getKeyspaceInstance(keyspaceName);
        if (keyspace == null)
        {
            return false;
        }

        try
        {
            keyspace.getColumnFamilyStore(tableName);
        }
        catch (IllegalArgumentException exception)
        {
            LOGGER.info("Table instance does not exist. keyspace={} table={} existingCFS={}",
                        keyspace, tableName, keyspace.getColumnFamilyStores());
            return false;
        }
        return true;
    }

    // Check whether keyspace metadata exists. Create keyspace metadata, if not.
    // Check whether keyspace instance is opened. Open the keyspace, if not.
    // NOTE: It is possible that external code that just creates metadata, but does not open the keyspace
    private static void setupKeyspace(Schema schema,
                                      String keyspaceName,
                                      ReplicationFactor replicationFactor,
                                      IPartitioner partitioner)
    {
        if (!keyspaceMetadataExists(schema, keyspaceName))
        {
            LOGGER.info("Setting up keyspace metadata in schema keyspace={} rfStrategy={} partitioner={}",
                        keyspaceName, replicationFactor.getReplicationStrategy().name(), partitioner);
            KeyspaceMetadata keyspaceMetadata =
                    KeyspaceMetadata.create(keyspaceName, KeyspaceParams.create(true, rfToMap(replicationFactor)));
            schema.load(keyspaceMetadata);
        }

        if (!keyspaceInstanceExists(schema, keyspaceName))
        {
            LOGGER.info("Setting up keyspace instance in schema keyspace={} rfStrategy={} partitioner={}",
                        keyspaceName, replicationFactor.getReplicationStrategy().name(), partitioner);
            // Create keyspace instance and also initCf (cfs) for the table
            Keyspace.openWithoutSSTables(keyspaceName);
        }
    }

    // Check whether table metadata exists. Create table metadata, if not.
    // Check whether table instance is opened. Open/init the table, if not.
    // NOTE: It is possible that external code that just creates metadata, but does not open the table
    private static void setupTableAndUdt(Schema schema,
                                         String keyspaceName,
                                         TableMetadata tableMetadata,
                                         Types userTypes)
    {
        String tableName = tableMetadata.name;
        KeyspaceMetadata keyspaceMetadata = schema.getKeyspaceMetadata(keyspaceName);
        if (keyspaceMetadata == null)
        {
            LOGGER.error("Keyspace metadata does not exist. keyspace={}", keyspaceName);
            throw new IllegalStateException("Keyspace metadata null for '" + keyspaceName
                                          + "' when it should have been initialized already");
        }

        if (!tableMetadataExists(schema, keyspaceName, tableName))
        {
            LOGGER.info("Setting up table metadata in schema keyspace={} table={} partitioner={}",
                        keyspaceName, tableName, tableMetadata.partitioner.getClass().getName());
            keyspaceMetadata = keyspaceMetadata.withSwapped(keyspaceMetadata.tables.with(tableMetadata));
            schema.load(keyspaceMetadata);
        }

        // The metadata of the table might not be the input tableMetadata. Fetch the current to be safe.
        TableMetadata currentTable = schema.getTableMetadata(keyspaceName, tableName);
        if (!tableInstanceExists(schema, keyspaceName, tableName))
        {
            LOGGER.info("Setting up table instance in schema keyspace={} table={} partitioner={}",
                        keyspaceName, tableName, tableMetadata.partitioner.getClass().getName());
            if (keyspaceInstanceExists(schema, keyspaceName))
            {
                // initCf (cfs) in the opened keyspace
                schema.getKeyspaceInstance(keyspaceName)
                      .initCf(TableMetadataRef.forOfflineTools(currentTable), false);
            }
            else
            {
                // The keyspace has not yet opened, create/open keyspace instance and also initCf (cfs) for the table
                Keyspace.openWithoutSSTables(keyspaceName);
            }
        }

        if (!userTypes.equals(Types.none()))
        {
            LOGGER.info("Setting up user types in schema keyspace={} types={}",
                        keyspaceName, userTypes);
            // Update Schema instance with any user-defined types built
            keyspaceMetadata = keyspaceMetadata.withSwapped(userTypes);
            schema.load(keyspaceMetadata);
        }
    }

    private static Pair<KeyspaceMetadata, TableMetadata> validateKeyspaceTable(Schema schema,
                                                                               String keyspaceName,
                                                                               String tableName)
    {
        Preconditions.checkState(keyspaceMetadataExists(schema, keyspaceName),
                                 "Keyspace metadata does not exist after building schema. keyspace=%s",
                                 keyspaceName);
        Preconditions.checkState(keyspaceInstanceExists(schema, keyspaceName),
                                 "Keyspace instance is not opened after building schema. keyspace=%s",
                                 keyspaceName);
        Preconditions.checkState(tableMetadataExists(schema, keyspaceName, tableName),
                                 "Table metadata does not exist after building schema. keyspace=%s table=%s",
                                 keyspaceName, tableName);
        Preconditions.checkState(tableInstanceExists(schema, keyspaceName, tableName),
                                 "Table instance is not opened after building schema. keyspace=%s table=%s",
                                 keyspaceName, tableName);

        // Validated above that keyspace and table, both exist and are opened
        KeyspaceMetadata keyspaceMetadata = schema.getKeyspaceMetadata(keyspaceName);
        TableMetadata tableMetadata = schema.getTableMetadata(keyspaceName, tableName);
        return Pair.create(keyspaceMetadata, tableMetadata);
    }

    public TableMetadata tableMetaData()
    {
        return metadata;
    }

    public String createStatement()
    {
        return createStmt;
    }

    public CqlTable build()
    {
        Map<String, CqlField.CqlUdt> udts = buildsUdts(keyspaceMetadata);
        List<CqlField> fields = buildFields(metadata, udts).stream().sorted().collect(Collectors.toList());
        return new CqlTable(keyspace,
                            metadata.name,
                            createStmt,
                            replicationFactor,
                            fields,
                            new HashSet<>(udts.values()),
                            indexCount);
    }

    private Map<String, CqlField.CqlUdt> buildsUdts(KeyspaceMetadata keyspaceMetadata)
    {
        List<UserType> userTypes = new ArrayList<>();
        keyspaceMetadata.types.forEach(userTypes::add);
        Map<String, CqlField.CqlUdt> udts = new HashMap<>(userTypes.size());
        while (!userTypes.isEmpty())
        {
            UserType userType = userTypes.remove(0);
            if (!SchemaBuilder.nestedUdts(userType).stream().allMatch(udts::containsKey))
            {
                // This UDT contains a nested user-defined type that has not been parsed yet
                // so re-add to the queue and parse later
                userTypes.add(userType);
                continue;
            }
            String name = userType.getNameAsString();
            CqlUdt.Builder builder = CqlUdt.builder(keyspaceMetadata.name, name);
            for (int field = 0; field < userType.size(); field++)
            {
                builder.withField(userType.fieldName(field).toString(),
                                  cassandraTypes.parseType(userType.fieldType(field).asCQL3Type().toString(), udts));
            }
            udts.put(name, builder.build());
        }

        return udts;
    }

    /**
     * @param type an abstract type
     * @return a set of UDTs nested within the type parameter
     */
    private static Set<String> nestedUdts(AbstractType<?> type)
    {
        Set<String> result = new HashSet<>();
        nestedUdts(type, result, false);
        return result;
    }

    private static void nestedUdts(AbstractType<?> type, Set<String> udts, boolean isNested)
    {
        if (type instanceof UserType)
        {
            if (isNested)
            {
                udts.add(((UserType) type).getNameAsString());
            }
            for (AbstractType<?> nestedType : ((UserType) type).fieldTypes())
            {
                nestedUdts(nestedType, udts, true);
            }
        }
        else if (type instanceof TupleType)
        {
            for (AbstractType<?> nestedType : ((TupleType) type).allTypes())
            {
                nestedUdts(nestedType, udts, true);
            }
        }
        else if (type instanceof SetType)
        {
            nestedUdts(((SetType<?>) type).getElementsType(), udts, true);
        }
        else if (type instanceof ListType)
        {
            nestedUdts(((ListType<?>) type).getElementsType(), udts, true);
        }
        else if (type instanceof MapType)
        {
            nestedUdts(((MapType<?, ?>) type).getKeysType(), udts, true);
            nestedUdts(((MapType<?, ?>) type).getValuesType(), udts, true);
        }
    }

    private List<CqlField> buildFields(TableMetadata metadata, Map<String, CqlField.CqlUdt> udts)
    {
        Iterator<ColumnMetadata> it = metadata.allColumnsInSelectOrder();
        List<CqlField> result = new ArrayList<>();
        int position = 0;
        while (it.hasNext())
        {
            ColumnMetadata col = it.next();
            boolean isPartitionKey = col.isPartitionKey();
            boolean isClusteringColumn = col.isClusteringColumn();
            boolean isStatic = col.isStatic();
            String name = col.name.toString();
            CqlField.CqlType type = col.type.isUDT() ? udts.get(((UserType) col.type).getNameAsString())
                                                     : cassandraTypes.parseType(col.type.asCQL3Type().toString(), udts);
            boolean isFrozen = col.type.isFreezable() && !col.type.isMultiCell();
            result.add(new CqlField(isPartitionKey,
                                    isClusteringColumn,
                                    isStatic,
                                    name,
                                    !(type instanceof CqlFrozen) && isFrozen ? CqlFrozen.build(type) : type,
                                    position));
            position++;
        }
        return result;
    }

    static Map<String, String> rfToMap(ReplicationFactor replicationFactor)
    {
        Map<String, String> result = new HashMap<>(replicationFactor.getOptions().size() + 1);
        result.put("class", "org.apache.cassandra.locator." + replicationFactor.getReplicationStrategy().name());
        for (Map.Entry<String, Integer> entry : replicationFactor.getOptions().entrySet())
        {
            result.put(entry.getKey(), Integer.toString(entry.getValue()));
        }
        return result;
    }
}
