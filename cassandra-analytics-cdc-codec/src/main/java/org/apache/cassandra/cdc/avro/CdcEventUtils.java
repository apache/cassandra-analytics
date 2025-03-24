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

package org.apache.cassandra.cdc.avro;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.cassandra.cdc.api.KeyspaceTypeKey;
import org.apache.cassandra.cdc.avro.msg.FieldValue;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.msg.RangeTombstone;
import org.apache.cassandra.cdc.msg.Value;
import org.apache.cassandra.cdc.schemastore.SchemaStore;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.Preconditions;

import static org.apache.cassandra.spark.utils.MapUtils.mapOf;

public final class CdcEventUtils
{
    private CdcEventUtils()
    {

    }

    public enum OperationType
    {
        UPDATE, INSERT, DELETE, DELETE_RANGE, DELETE_PARTITION
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(CdcEventUtils.class);

    // Determine the operation type given the row and the updated fields.
    // See cdc/src/resources/cdc.avsc for the defined variants.
    // Possible outcomes:
    // - INSERT, insertion
    // - UPDATE, update or cell deletion
    // - DELETE, row deletion
    // - DELETE_RANGE, range deletion
    // - DELETE_PARTITION, partition deletion
    public static GenericData.EnumSymbol getAvroOperationType(CdcEvent event, Schema avroSchema)
    {
        return new GenericData.EnumSymbol(avroSchema.getField("operationType").schema(),
                                          getOperationType(event));
    }

    public static <EventType extends CdcEvent>
    OperationType getOperationType(EventType event)
    {
        switch (event.getKind())
        {
            case INSERT:
                return OperationType.INSERT;
            case UPDATE:
                return OperationType.UPDATE;
            case DELETE:
            case ROW_DELETE:
            case COMPLEX_ELEMENT_DELETE: // todo: distinguish the deletions. Require avro schema update.
                return OperationType.DELETE;
            case RANGE_DELETE:
                return OperationType.DELETE_RANGE;
            case PARTITION_DELETE:
                return OperationType.DELETE_PARTITION;
            default:
                throw new IllegalStateException("Unknown CDC event kind: " + event.getKind());
        }
    }

    // return the list of field names that have data present in the row
    public static List<String> updatedFieldNames(CdcEvent event)
    {
        return updatedFields(event).stream()
                                   .map(v -> v.columnName)
                                   .collect(Collectors.toList());
    }

    // todo: seems a good candidate to be moved up to AbstractCdcEvent
    public static List<Value> updatedFields(CdcEvent event)
    {
        List<Value> result = new ArrayList<>();
        Consumer<List<Value>> addAllIfNotNull = list -> {
            if (list != null)
            {
                result.addAll(list);
            }
        };
        addAllIfNotNull.accept(event.getPartitionKeys());
        addAllIfNotNull.accept(event.getStaticColumns());
        addAllIfNotNull.accept(event.getClusteringKeys());
        addAllIfNotNull.accept(event.getValueColumns());
        return result;
    }

    public static List<GenericData.Record> getRangeTombstoneAvro(CdcEvent event, Schema rangeSchema,
                                                                 Function<Value, Object> avroFieldEncoder)
    {
        List<Map<String, Object>> range = getRangeTombstone(event, avroFieldEncoder);
        if (range == null || range.isEmpty())
        {
            return null;
        }

        return range.stream().map(tuple -> {
            GenericData.Record rangeRecord = new GenericData.Record(rangeSchema);
            rangeRecord.put("field", tuple.get("field"));
            rangeRecord.put("rangePredicateType", new GenericData.EnumSymbol(rangeSchema, tuple.get("rangePredicateType")));
            rangeRecord.put("value", tuple.get("value"));
            return rangeRecord;
        }).collect(Collectors.toList());
    }

    // Returns a list of predicates of clustering keys in the clustering key definition order,
    // e.g. for ck1 and ck2, the predicates of ck1 are placed before the ones for ck2
    public static List<Map<String, Object>>
    getRangeTombstone(CdcEvent event, Function<Value, Object> encoder)
    {
        List<RangeTombstone> rangeTombstones = event.getRangeTombstoneList();
        if (rangeTombstones == null || rangeTombstones.isEmpty())
        {
            return null;
        }

        List<Map<String, Object>> result = new ArrayList<>();
        int prefix = -1;
        for (RangeTombstone rt : rangeTombstones)
        {
            List<Value> startBounds = rt.getStartBound();
            List<Value> endBounds = rt.getEndBound();
            int p = findLongestPrefix(startBounds, endBounds);
            if (prefix == -1)
            {
                prefix = p;
            }
            if (p != prefix)
            {
                // prefix is different. It is from a batch that combines multiple deletes.
                // not handling it at the moment.
                LOGGER.warn("Not handling disjointed range deletion as it requires OR operator.");
                return null;
            }
        }

        // IN relation only exists within [0, prefix)
        // How to determine if columns are in the IN clause?
        // We collect the values of each column from the range tombstones.
        // If the column has more than 1 values, we need to use IN.
        // Otherwise, it is EQ. If there is a single range tombstone, it only has EQ.
        List<Set<FieldValue>> uniqueClusteringPrefixValues = Stream.generate(() -> new HashSet<FieldValue>())
                                                                   .limit(prefix)
                                                                   .collect(Collectors.toList());
        // collect the unique values of each clustering key upto the prefix position
        for (RangeTombstone rt : rangeTombstones)
        {
            List<Value> bound = rt.getStartBound();
            for (int i = 0; i < prefix; i++)
            {
                FieldValue value = new FieldValue(bound.get(i));
                Set<FieldValue> uniqeValues = uniqueClusteringPrefixValues.get(i);
                uniqeValues.add(value);
            }
        }
        // determine the relation. It is IN if there are multiple unique values; if there is just one, it is EQ.
        for (Set<FieldValue> valueSet : uniqueClusteringPrefixValues)
        {
            for (FieldValue fv : valueSet)
            {
                result.add(makeRangePredicate(fv.value.columnName,
                                              valueSet.size() > 1 ? "IN" : "EQ",
                                              encoder.apply(fv.value)));
            }
        }

        // After finishing the EQ / IN parts, now we are at the actual range part.
        // The values are the same across all range tombstones. So we can just pick the first one.
        RangeTombstone firstRT = rangeTombstones.get(0);
        boolean startInclusive = firstRT.startInclusive;
        boolean endInclusive = firstRT.endInclusive;
        List<Value> start = firstRT.getStartBound();
        List<Value> end = firstRT.getEndBound();
        int index = prefix;
        int longest = Math.max(start.size(), end.size());
        while (index < longest)
        {
            if (index < start.size())
            {
                Value v = start.get(index);
                result.add(makeRangePredicate(v.columnName,
                                              startInclusive ? "GTE" : "GT",
                                              encoder.apply(v)));
            }
            if (index < end.size())
            {
                Value v = end.get(index);
                result.add(makeRangePredicate(v.columnName,
                                              endInclusive ? "LTE" : "LT",
                                              encoder.apply(v)));
            }
            index++;
        }

        return result;
    }

    private static Map<String, Object> makeRangePredicate(String columnName, String predicateType, Object value)
    {
        return mapOf("field", columnName, "rangePredicateType", predicateType, "value", value);
    }

    // return index of the longest prefix (exclusive)
    private static int findLongestPrefix(List<Value> bound1, List<Value> bound2)
    {
        int s1 = bound1.size();
        int s2 = bound2.size();
        int res = 0;
        for (int i = 0, j = 0; i < s1 && j < s2; i++, j++, res++)
        {
            Value v1 = bound1.get(i);
            Value v2 = bound2.get(j);
            if (!valueMatches(v1, v2))
            {
                return res;
            }
        }
        return res;
    }

    private static boolean valueMatches(Value v1, Value v2)
    {
        return Objects.equals(v1.columnName, v2.columnName)
               && Objects.equals(v1.columnType, v2.columnType)
               && Objects.equals(v1.getValue(), v2.getValue());
    }

    /**
     * Generate the ttl record from row. If ttl is absent, null is returned.
     *
     * @param event     cdc event
     * @param ttlSchema Avro schema for the ttl value.
     * @return record for the ttl value holding the ttl in seconds and deletedAt timestamp.
     */
    public static GenericData.Record getTTLAvro(CdcEvent event, Schema ttlSchema)
    {
        CdcEvent.TimeToLive ttl = event.getTtl();
        if (ttl == null)
        {
            return null;
        }

        GenericData.Record ttlRecord = new GenericData.Record(ttlSchema);
        ttlRecord.put("ttl", ttl.ttlInSec);
        ttlRecord.put("deletedAt", ttl.expirationTimeInSec);
        return ttlRecord;
    }

    public static Map<String, Integer> getTTL(CdcEvent event)
    {
        CdcEvent.TimeToLive ttl = event.getTtl();
        if (ttl == null)
        {
            return null;
        }
        return mapOf("ttl", ttl.ttlInSec, "deletedAt", ttl.expirationTimeInSec);
    }

    public static UpdatedEvent getUpdatedEvent(CdcEvent event,
                                               SchemaStore store,
                                               int truncateThreshold,
                                               Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup)
    {
        Schema tableSchema = store.getSchema(event.keyspace + '.' + event.table, null);
        List<String> truncatedFields = new ArrayList<>();
        GenericData.Record update = new GenericData.Record(tableSchema);
        int totalSize = 0;

        for (Value field : CdcEventUtils.updatedFields(event))
        {
            ByteBuffer value = field.getValue();
            if (value == null) // the field is deleted
            {
                update.put(field.columnName, null);
                continue;
            }

            if (totalSize + value.remaining() > truncateThreshold)
            {
                truncatedFields.add(field.columnName);
                continue;
            }

            totalSize += value.remaining();

            Schema.Field column = tableSchema.getField(field.columnName);
            Preconditions.checkNotNull(column,
                                       "Encountered an unknown field during event encoding. " +
                                       "Field: %s. Avro schema: %s", field.columnName, tableSchema.getFullName());
            CqlField.CqlType type = typeLookup.apply(KeyspaceTypeKey.of(event.keyspace, field.columnType));
            Object javaValue = type.deserializeToJavaType(value);
            update.put(field.columnName, AvroDataUtils.toAvro(javaValue, column.schema()));
        }
        return new UpdatedEvent(update, truncatedFields);
    }

    public static class UpdatedEvent
    {
        private GenericData.Record record;
        private List<String> truncatedFields;

        public UpdatedEvent(GenericData.Record record, List<String> truncatedFields)
        {
            this.record = record;
            this.truncatedFields = truncatedFields;
        }

        public GenericData.Record getRecord()
        {
            return record;
        }

        public List<String> getTruncatedFields()
        {
            return truncatedFields;
        }
    }
}
