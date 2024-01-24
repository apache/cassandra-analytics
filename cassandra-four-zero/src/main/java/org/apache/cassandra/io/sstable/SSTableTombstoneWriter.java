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

package org.apache.cassandra.io.sstable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.bridge.CassandraSchema;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.cql3.functions.UDHelper;
import org.apache.cassandra.cql3.functions.types.TypeCodec;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTypeStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.schema.Functions;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.Views;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Re-write of CQLSSTableWriter for writing tombstones to an SSTable for testing
 * Used for testing purpose only
 */
@VisibleForTesting
public final class SSTableTombstoneWriter implements Closeable
{
    private static final ByteBuffer UNSET_VALUE = ByteBufferUtil.UNSET_BYTE_BUFFER;

    static
    {
        DatabaseDescriptor.clientInitialization(false);
        // Partitioner is not set in client mode
        if (DatabaseDescriptor.getPartitioner() == null)
        {
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        }
    }

    private final AbstractSSTableSimpleWriter writer;
    private final DeleteStatement delete;
    private final List<ColumnSpecification> boundNames;
    private final List<TypeCodec> typeCodecs;
    private final ClusteringComparator comparator;

    private SSTableTombstoneWriter(AbstractSSTableSimpleWriter writer,
                                   DeleteStatement delete,
                                   List<ColumnSpecification> boundNames,
                                   ClusteringComparator comparator)
    {
        this.writer = writer;
        this.delete = delete;
        this.boundNames = boundNames;
        this.typeCodecs = boundNames.stream().map(bn -> UDHelper.codecFor(UDHelper.driverType(bn.type)))
                                    .collect(Collectors.toList());
        this.comparator = comparator;
    }

    /**
     * Returns a new builder for a SSTableTombstoneWriter
     *
     * @return the new builder
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Adds a new row to the writer.
     *
     * This is a shortcut for {@code addRow(Arrays.asList(values))}.
     *
     * @param values the row values (corresponding to the bind variables of the
     *               deletion statement used when creating by this writer)
     * @throws IOException when adding a row with the given {@code values} fails
     */
    public void addRow(Object... values) throws InvalidRequestException, IOException
    {
        addRow(Arrays.asList(values));
    }

    /**
     * Adds a new row to the writer.
     * <p>
     * Each provided value type should correspond to the types of the CQL column the value is for.
     * The correspondence between java type and CQL type is the same one than the one documented at
     * www.datastax.com/drivers/java/2.0/apidocs/com/datastax/driver/core/DataType.Name.html#asJavaClass().
     * <p>
     * If you prefer providing the values directly as binary, use
     *
     * @param values the row values (corresponding to the bind variables of the
     *               deletion statement used when creating by this writer)
     */
    private void addRow(List<Object> values) throws InvalidRequestException, IOException
    {
        int size = Math.min(values.size(), boundNames.size());
        List<ByteBuffer> rawValues = new ArrayList<>(size);

        for (int index = 0; index < size; index++)
        {
            Object value = values.get(index);
            rawValues.add(serialize(value, typeCodecs.get(index)));
        }

        rawAddRow(rawValues);
    }

    /**
     * Adds a new row to the writer given already serialized values.
     *
     * This is a shortcut for {@code rawAddRow(Arrays.asList(values))}.
     *
     * @param values the row values (corresponding to the bind variables of the
     *               deletion statement used when creating by this writer) as binary
     */
    private void rawAddRow(List<ByteBuffer> values) throws InvalidRequestException, IOException
    {
        if (values.size() != boundNames.size())
        {
            throw new InvalidRequestException(
                    String.format("Invalid number of arguments, expecting %d values but got %d",
                                  boundNames.size(), values.size()));
        }

        QueryOptions options = QueryOptions.forInternalCalls(null, values);
        List<ByteBuffer> keys = delete.buildPartitionKeyNames(options);

        long now = System.currentTimeMillis();
        // NOTE: We ask indexes to not validate values (the last 'false' arg below) because that
        //       triggers a 'Keyspace.open' and that forces a lot of initialization that we don't want
        UpdateParameters params = new UpdateParameters(delete.metadata,
                                                       delete.updatedColumns(),
                                                       options,
                                                       delete.getTimestamp(TimeUnit.MILLISECONDS.toMicros(now), options),
                                                       (int) TimeUnit.MILLISECONDS.toSeconds(now),
                                                       delete.getTimeToLive(options),
                                                       Collections.emptyMap());

        if (delete.hasSlices())
        {
            // Write out range tombstones
            SortedSet<ClusteringBound<?>> startBounds = delete.getRestrictions().getClusteringColumnsBounds(Bound.START, options);
            SortedSet<ClusteringBound<?>> endBounds = delete.getRestrictions().getClusteringColumnsBounds(Bound.END, options);
            Slices slices = toSlices(startBounds, endBounds);

            try
            {
                for (ByteBuffer key : keys)
                {
                    for (Slice slice : slices)
                    {
                        delete.addUpdateForKey(writer.getUpdateFor(key), slice, params);
                    }
                }
                return;
            }
            catch (SSTableSimpleUnsortedWriter.SyncException exception)
            {
                // If we use a BufferedWriter and had a problem writing to disk, the IOException has been
                // wrapped in a SyncException (see BufferedWriter below). We want to extract that IOException.
                throw (IOException) exception.getCause();
            }
        }

        SortedSet<Clustering<?>> clusterings = delete.createClustering(options);
        try
        {
            for (ByteBuffer key : keys)
            {
                for (Clustering<?> clustering : clusterings)
                {
                    delete.addUpdateForKey(writer.getUpdateFor(key), clustering, params);
                }
            }
        }
        catch (SSTableSimpleUnsortedWriter.SyncException exception)
        {
            // If we use a BufferedWriter and had a problem writing to disk, the IOException has been
            // wrapped in a SyncException (see BufferedWriter below). We want to extract that IOException.
            throw (IOException) exception.getCause();
        }
    }

    private Slices toSlices(SortedSet<ClusteringBound<?>> startBounds, SortedSet<ClusteringBound<?>> endBounds)
    {
        assert startBounds.size() == endBounds.size();

        Slices.Builder builder = new Slices.Builder(comparator);

        Iterator<ClusteringBound<?>> starts = startBounds.iterator();
        Iterator<ClusteringBound<?>> ends = endBounds.iterator();

        while (starts.hasNext())
        {
            Slice slice = Slice.make(starts.next(), ends.next());
            if (!slice.isEmpty(comparator))
            {
                builder.add(slice);
            }
        }

        return builder.build();
    }

    /**
     * Close this writer.
     * <p>
     * This method should be called, otherwise the produced SSTables are not
     * guaranteed to be complete (and won't be in practice).
     */
    public void close() throws IOException
    {
        writer.close();
    }

    @SuppressWarnings("unchecked")
    private ByteBuffer serialize(Object value, TypeCodec codec)
    {
        if (value == null || value == UNSET_VALUE)
        {
            return (ByteBuffer) value;
        }

        return codec.serialize(value, ProtocolVersion.CURRENT);
    }

    /**
     * A Builder for a SSTableTombstoneWriter object
     */
    public static class Builder
    {
        private File directory;

        SSTableFormat.Type formatType = null;

        private CreateTableStatement.Raw schemaStatement;
        private final List<CreateTypeStatement.Raw> typeStatements;
        private ModificationStatement.Parsed deleteStatement;
        private IPartitioner partitioner;

        private long bufferSizeInMB = 128;

        Builder()
        {
            this.typeStatements = new ArrayList<>();
        }

        /**
         * The directory where to write the SSTables (mandatory option).
         *
         * This is a mandatory option.
         *
         * @param directory the directory to use, which should exists and be writable
         * @return this builder
         * @throws IllegalArgumentException if {@code directory} doesn't exist or is not writable
         */
        public Builder inDirectory(File directory)
        {
            if (!directory.exists())
            {
                throw new IllegalArgumentException(directory + " doesn't exists");
            }
            if (!directory.canWrite())
            {
                throw new IllegalArgumentException(directory + " exists but is not writable");
            }

            this.directory = directory;
            return this;
        }

        /**
         * The schema (CREATE TABLE statement) for the table for which SSTable are to be created.
         * <p>
         * Please note that the provided CREATE TABLE statement <b>must</b> use a fully-qualified
         * table name, one that include the keyspace name.
         * <p>
         * This is a mandatory option.
         *
         * @param schema the schema of the table for which SSTables are to be created
         * @return this builder
         * @throws IllegalArgumentException if {@code schema} is not a valid CREATE TABLE statement
         *                                  or does not have a fully-qualified table name
         */
        public Builder forTable(String schema)
        {
            schemaStatement = QueryProcessor.parseStatement(schema, CreateTableStatement.Raw.class, "CREATE TABLE");
            return this;
        }

        /**
         * The partitioner to use.
         * <p>
         * By default, {@code Murmur3Partitioner} will be used. If this is not the partitioner used
         * by the cluster for which the SSTables are created, you need to use this method to
         * provide the correct partitioner.
         *
         * @param partitioner the partitioner to use
         * @return this builder
         */
        public Builder withPartitioner(IPartitioner partitioner)
        {
            this.partitioner = partitioner;
            return this;
        }

        /**
         * The DELETE statement defining the values to remove for a given CQL row.
         * <p>
         * Please note that the provided DELETE statement <b>must</b> use a fully-qualified
         * table name, one that include the keyspace name. Moreover, said statement must use
         * bind variables since these variables will be bound to values by the resulting writer.
         * <p>
         * This is a mandatory option.
         *
         * @param delete a delete statement that defines the order of column values to use
         * @return this builder
         * @throws IllegalArgumentException if {@code deleteStatement} is not a valid deletion statement,
         *                                  does not have a fully-qualified table name or have no bind variables
         */
        public Builder using(String delete)
        {
            deleteStatement = QueryProcessor.parseStatement(delete, ModificationStatement.Parsed.class, "DELETE");
            return this;
        }

        /**
         * The size of the buffer to use.
         * <p>
         * This defines how much data will be buffered before being written as
         * a new SSTable. This correspond roughly to the data size that will have the created
         * SSTable.
         * <p>
         * The default is 128MB, which should be reasonable for a 1GB heap. If you experience
         * OOM while using the writer, you should lower this value.
         *
         * @param size the size to use in MB
         * @return this builder
         */
        public Builder withBufferSizeInMB(int size)
        {
            bufferSizeInMB = size;
            return this;
        }

        public SSTableTombstoneWriter build()
        {
            if (directory == null)
            {
                throw new IllegalStateException("No ouptut directory specified, you should provide a directory with inDirectory()");
            }
            if (schemaStatement == null)
            {
                throw new IllegalStateException("Missing schema, you should provide the schema for the SSTable to create with forTable()");
            }
            if (deleteStatement == null)
            {
                throw new IllegalStateException("No delete statement specified, you should provide a delete statement through using()");
            }

            TableMetadata tableMetadata = CassandraSchema.apply(schema -> {
                if (schema.getKeyspaceMetadata(SchemaConstants.SYSTEM_KEYSPACE_NAME) == null)
                {
                    schema.load(SystemKeyspace.metadata());
                }

                String keyspaceName = schemaStatement.keyspace();

                if (schema.getKeyspaceMetadata(keyspaceName) == null)
                {
                    schema.load(KeyspaceMetadata.create(keyspaceName,
                                                        KeyspaceParams.simple(1),
                                                        Tables.none(),
                                                        Views.none(),
                                                        Types.none(),
                                                        Functions.none()));
                }

                KeyspaceMetadata ksm = schema.getKeyspaceMetadata(keyspaceName);

                TableMetadata table = ksm.tables.getNullable(schemaStatement.table());
                if (table == null)
                {
                    Types types = createTypes(keyspaceName);
                    table = createTable(types);
                    schema.load(ksm.withSwapped(ksm.tables.with(table)).withSwapped(types));
                }
                return table;
            });

            DeleteStatement preparedDelete = prepareDelete();
            TableMetadataRef ref = TableMetadataRef.forOfflineTools(tableMetadata);
            AbstractSSTableSimpleWriter writer = new SSTableSimpleUnsortedWriter(directory, ref,
                                                                                 preparedDelete.updatedColumns(),
                                                                                 bufferSizeInMB);

            if (formatType != null)
            {
                writer.setSSTableFormatType(formatType);
            }

            return new SSTableTombstoneWriter(writer, preparedDelete, preparedDelete.getBindVariables(), tableMetadata.comparator);
        }

        private Types createTypes(String keyspace)
        {
            Types.RawBuilder builder = Types.rawBuilder(keyspace);
            for (CreateTypeStatement.Raw st : typeStatements)
            {
                st.addToRawBuilder(builder);
            }
            return builder.build();
        }

        /**
         * Creates the table according to schema statement
         *
         * @param types types this table should be created with
         */
        private TableMetadata createTable(Types types)
        {
            ClientState state = ClientState.forInternalCalls();
            CreateTableStatement statement = schemaStatement.prepare(state);
            statement.validate(ClientState.forInternalCalls());

            TableMetadata.Builder builder = statement.builder(types);
            if (partitioner != null)
            {
                builder.partitioner(partitioner);
            }

            return builder.build();
        }

        /**
         * Prepares delete statement for writing data to SSTable
         *
         * @return prepared Delete statement and it's bound names
         */
        private DeleteStatement prepareDelete()
        {
            ClientState state = ClientState.forInternalCalls();
            DeleteStatement delete = (DeleteStatement) deleteStatement.prepare(state);
            delete.validate(state);

            if (delete.hasConditions())
            {
                throw new IllegalArgumentException("Conditional statements are not supported");
            }
            if (delete.isCounter())
            {
                throw new IllegalArgumentException("Counter update statements are not supported");
            }
            if (delete.getBindVariables().isEmpty())
            {
                throw new IllegalArgumentException("Provided delete statement has no bind variables");
            }

            return delete;
        }
    }
}
