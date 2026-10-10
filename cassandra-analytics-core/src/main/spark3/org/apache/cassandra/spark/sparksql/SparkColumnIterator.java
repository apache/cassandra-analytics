package org.apache.cassandra.spark.sparksql;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;

public class SparkColumnIterator implements PartitionReader<ColumnarBatch>
{
    private static final String VECTORS_BATCH_SIZE_PARAM_NAME = "vectors.batch.size";
    private static final String VECTORS_OFF_HEAP_ENABLED = "vectors.offheap.enabled";
    private static final int DEFAULT_BATCH_SIZE_VALUE = 4096;
    private static final boolean DEFAULT_VECTORS_OFF_HEAP_VALUE = false;

    private final SparkRowIterator rowIterator;

    private final int batchSize;
    private final boolean offHeap;

    private final WritableColumnVector[] vectors;
    private final ColumnarBatch batch;
    private MappingFunction[] mappings;

    public SparkColumnIterator(CaseInsensitiveStringMap options,
                               int partitionId,
                               @NotNull DataLayer dataLayer,
                               @Nullable StructType requiredSchema,
                               @NotNull List<PartitionKeyFilter> partitionKeyFilters)
    {
        batchSize = options.getInt(VECTORS_BATCH_SIZE_PARAM_NAME, DEFAULT_BATCH_SIZE_VALUE);
        offHeap = options.getBoolean(VECTORS_OFF_HEAP_ENABLED, DEFAULT_VECTORS_OFF_HEAP_VALUE);

        rowIterator = new SparkRowIterator(partitionId, dataLayer, requiredSchema, partitionKeyFilters);

        StructType schema = requiredSchema != null ? requiredSchema : dataLayer.structType();
        vectors = getVectors(offHeap, schema);

        batch = new ColumnarBatch(vectors);
        initializeMappings();
    }

    @Override
    public boolean next() throws IOException
    {
        for (WritableColumnVector v : vectors)
            v.reset();

        int rowId = 0;
        while (rowId < batchSize && rowIterator.next())
        {
            InternalRow row = rowIterator.get();

            for (int colIdx = 0; colIdx < vectors.length; colIdx++)
            {
                if (row.isNullAt(colIdx))
                    vectors[colIdx].putNull(rowId);
                else
                    mappings[colIdx].apply(vectors[colIdx], row, colIdx, rowId);
            }
            rowId++;
        }

        batch.setNumRows(rowId);
        return rowId > 0;
    }

    @Override
    public ColumnarBatch get()
    {
        return batch;
    }

    @Override
    public void close() throws IOException
    {
        rowIterator.close();

        for (WritableColumnVector v : vectors)
            v.close();

        batch.close();
    }

    interface MappingFunction
    {
        void apply(WritableColumnVector vector, InternalRow row, int colIdx, int rowId);
    }

    private void initializeMappings()
    {
        mappings = new MappingFunction[vectors.length];
        for (int i = 0; i < vectors.length; i++)
        {
            DataType dataType = vectors[i].dataType();
            if (dataType == DataTypes.IntegerType)
                mappings[i] = (vector, row, colIdx, rowId) -> vector.putInt(rowId, row.getInt(colIdx));
            else if (dataType == DataTypes.FloatType)
                mappings[i] = (vector, row, colIdx, rowId) -> vector.putFloat(rowId, row.getFloat(colIdx));
            else if (dataType == DataTypes.StringType)
                mappings[i] = (vector, row, colIdx, rowId) -> vector.putByteArray(rowId, row.getUTF8String(colIdx).getBytes());
        }
    }

    private WritableColumnVector[] getVectors(boolean offHeap, StructType schema)
    {
        if (offHeap)
            return OffHeapColumnVector.allocateColumns(batchSize, schema);
        else
            return OnHeapColumnVector.allocateColumns(batchSize, schema);
    }
}
