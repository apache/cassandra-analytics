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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.apache.cassandra.spark.data.SchemaFeatureCustomizer.findTtlAndTimestampAwareCqlField;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchemaFeatureCustomizerTest
{
    @Test
    void testFindCqlFieldExactMatch()
    {
        CqlTable table = mock(CqlTable.class);
        CqlField column1 = mock(CqlField.class);

        when(table.getField("column1")).thenReturn(column1);
        when(column1.isPrimaryKey()).thenReturn(false);

        CqlField result = findTtlAndTimestampAwareCqlField(table, "column1", "cell_ttl");
        assertThat(result).isSameAs(column1);
    }

    @Test
    void testFindCqlFieldCaseInsensitiveMatch()
    {
        CqlTable table = mock(CqlTable.class);
        CqlField column = new CqlField(false, false, false, "Column1", mock(CqlField.CqlType.class), 0);

        when(table.getField("column1")).thenReturn(null);
        when(table.fields()).thenReturn(Collections.singletonList(column));

        CqlField result = findTtlAndTimestampAwareCqlField(table, "column1", "cell_ttl");
        assertThat(result).isSameAs(column);
    }

    @Test
    void testFindCqlFieldRejectsMissingColumn()
    {
        CqlTable table = mock(CqlTable.class);
        CqlField column1 = new CqlField(false, false, false, "a", mock(CqlField.CqlType.class), 0);
        CqlField column2 = new CqlField(false, false, false, "b", mock(CqlField.CqlType.class), 1);

        when(table.getField("missing")).thenReturn(null);
        when(table.fields()).thenReturn(List.of(column1, column2));
        when(table.keyspace()).thenReturn("ks");
        when(table.table()).thenReturn("tbl");

        assertThatThrownBy(() -> findTtlAndTimestampAwareCqlField(table, "missing", "cell_ttl"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unable to enable schema feature 'cell_ttl': column 'missing' does not exist in table ks.tbl");
    }

    @Test
    void testFindCqlFieldRejectsAmbiguousCaseInsensitiveMatch()
    {
        CqlTable table = mock(CqlTable.class);
        CqlField upperCaseColumn = new CqlField(false, false, false, "Column", mock(CqlField.CqlType.class), 0);
        CqlField lowerCaseColumn = new CqlField(false, false, false, "column", mock(CqlField.CqlType.class), 0);

        when(table.getField("COLUMN")).thenReturn(null);
        when(table.fields()).thenReturn(Arrays.asList(upperCaseColumn, lowerCaseColumn));
        when(table.keyspace()).thenReturn("ks");
        when(table.table()).thenReturn("tbl");

        assertThatThrownBy(() -> findTtlAndTimestampAwareCqlField(table, "COLUMN", "cell_ttl"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unable to enable schema feature 'cell_ttl': column 'COLUMN' is ambiguous in table ks.tbl. " +
                    "Matching columns: Column, column");
    }

    @Test
    void testFindCqlFieldRejectsPrimaryKey()
    {
        CqlTable table = mock(CqlTable.class);
        CqlField idColumn = new CqlField(true, false, false, "id", mock(CqlField.CqlType.class), 0);

        when(table.getField("id")).thenReturn(idColumn);

        assertThatThrownBy(() -> findTtlAndTimestampAwareCqlField(table, "id", "cell_ttl"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unable to enable schema feature 'cell_ttl'")
        .hasMessageContaining("column 'id' is part of primary key");
    }
}
