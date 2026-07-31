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

package org.apache.cassandra.spark.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.jetbrains.annotations.NotNull;

/**
 * CQL-related utility methods
 */
public final class CqlUtils
{
    // Properties to be overridden when extracted from the table schema
    private static final List<String> TABLE_PROPERTY_OVERRIDE_ALLOWLIST = Arrays.asList("bloom_filter_fp_chance",
                                                                                        "compression",
                                                                                        "default_time_to_live",
                                                                                        "min_index_interval",
                                                                                        "max_index_interval"
                                                                                        );
    private static final Pattern REPLICATION_FACTOR_PATTERN = Pattern.compile("WITH REPLICATION = (\\{[^\\}]*\\})", Pattern.CASE_INSENSITIVE);
    // Initialize a mapper allowing single quotes to process the RF string from the CREATE KEYSPACE statement
    private static final ObjectMapper MAPPER = new ObjectMapper().configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    private static final Pattern ESCAPED_WHITESPACE_PATTERN = Pattern.compile("(\\\\r|\\\\n|\\\\r\\n)+");
    private static final Pattern NEWLINE_PATTERN = Pattern.compile("\n");
    private static final Pattern ESCAPED_DOUBLE_BACKSLASH = Pattern.compile("\\\\");
    private static final Pattern COMPACTION_STRATEGY_PATTERN = Pattern.compile("compaction\\s*=\\s*\\{\\s*'class'\\s*:\\s*'([^']+)'");

    private static final Pattern MULTI_WHITESPACE_PATTERN = Pattern.compile("\\s+");
    private static final Pattern SAI_USING_PATTERN =
        Pattern.compile("USING '(SAI|(ORG\\.APACHE\\.CASSANDRA\\.INDEX\\.SAI\\.)?STORAGEATTACHEDINDEX)'");

    private CqlUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static String cleanCql(@NotNull String cql)
    {
        String result = ESCAPED_WHITESPACE_PATTERN.matcher(cql).replaceAll("\n");
        result = NEWLINE_PATTERN.matcher(result).replaceAll("");
        result = ESCAPED_DOUBLE_BACKSLASH.matcher(result).replaceAll("");
        return result;
    }

    private static String removeTableProps(@NotNull String schema)
    {
        int index = schema.indexOf('(');
        int count = 1;
        if (index < 0)
        {
            throw new RuntimeException("Missing parentheses in table schema " + schema);
        }
        while (++index < schema.length())  // Find closing bracket
        {
            if (schema.charAt(index) == ')')
            {
                count--;
            }
            else if (schema.charAt(index) == '(')
            {
                count++;
            }
            if (count == 0)
            {
                break;
            }
        }
        if (count > 0)
        {
            throw new RuntimeException("Found unbalanced parentheses in table schema " + schema);
        }
        return schema.substring(0, index + 1);
    }

    public static Set<String> extractKeyspaceNames(@NotNull String schemaStr)
    {
        String cleaned = cleanCql(schemaStr);
        Pattern pattern = Pattern.compile("CREATE KEYSPACE \"?(\\w+)?\"? [^;]*;");
        Matcher matcher = pattern.matcher(cleaned);

        Set<String> keyspaces = new HashSet<>();
        while (matcher.find())
        {
            String keyspace = matcher.group(1);
            if (!keyspace.startsWith("system_") && !keyspace.startsWith("cie_"))
            {
                keyspaces.add(keyspace);
            }
        }
        return keyspaces;
    }

    public static String extractKeyspaceSchema(@NotNull String schemaStr, @NotNull String keyspace)
    {
        String cleaned = cleanCql(schemaStr);
        Pattern pattern = Pattern.compile(String.format("CREATE KEYSPACE \"?%s?\"? [^;]*;", keyspace));
        Matcher matcher = pattern.matcher(cleaned);

        if (!matcher.find())
        {
            throw new RuntimeException(String.format("Could not find schema for keyspace: %s", keyspace));
        }

        return cleaned.substring(matcher.start(0), matcher.end(0));
    }

    /**
     * @param schemaStr full cluster schema text.
     * @return map of keyspace/table identifier to table create statements.
     */
    public static Map<TableIdentifier, String> extractCdcTables(@NotNull final String schemaStr)
    {
        String cleaned = cleanCql(schemaStr);
        Pattern pattern = Pattern.compile("CREATE TABLE \"?(\\w+)\"?\\.\"?(\\w+)\"?[^;]*cdc = true[^;]*;");
        Matcher matcher = pattern.matcher(cleaned);
        Map<TableIdentifier, String> createStmts = new HashMap<>();
        while (matcher.find())
        {
            String keyspace = matcher.group(1);
            String table = matcher.group(2);
            createStmts.put(TableIdentifier.of(keyspace, table), extractCleanedTableSchema(cleaned, keyspace, table, false));
        }
        return createStmts;
    }

    public static ReplicationFactor extractReplicationFactor(@NotNull String schemaStr, @NotNull String keyspace)
    {
        String createKeyspaceSchema = extractKeyspaceSchema(schemaStr, keyspace);
        Matcher matcher = REPLICATION_FACTOR_PATTERN.matcher(createKeyspaceSchema);

        if (!matcher.find())
        {
            throw new RuntimeException(String.format("Could not find replication factor for keyspace: %s", keyspace));
        }

        Map<String, String> map;
        try
        {
            map = MAPPER.readValue(matcher.group(1), new TypeReference<Map<String, String>>() {});  // CHECKSTYLE IGNORE: Empty anonymous inner class
        }
        catch (IOException exception)
        {
            throw new RuntimeException(String.format("Unable to parse replication factor for keyspace: %s", keyspace), exception);
        }

        String className = map.remove("class");
        ReplicationFactor.ReplicationStrategy strategy = ReplicationFactor.ReplicationStrategy.getEnum(className);
        return new ReplicationFactor(strategy, map.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> Integer.parseInt(v.getValue()))));
    }

    public static String extractTableSchema(@NotNull String schemaStr, @NotNull String keyspace, @NotNull String table)
    {
        return extractCleanedTableSchema(cleanCql(schemaStr), keyspace, table, false);
    }

    public static String extractCleanedTableSchema(@NotNull String createStatementToClean,
                                                   @NotNull String keyspace,
                                                   @NotNull String table,
                                                   boolean withTableProps)
    {
        Pattern pattern = Pattern.compile(String.format("CREATE TABLE (IF NOT EXISTS)? ?\"?%s?\"?\\.{1}\"?%s\"?[^;]*;", keyspace, table));
        Matcher matcher = pattern.matcher(createStatementToClean);
        if (matcher.find())
        {
            String fullSchema = createStatementToClean.substring(matcher.start(0), matcher.end(0));
            String redactedSchema = withTableProps ? fullSchema : removeTableProps(fullSchema);
            String clustering = extractClustering(fullSchema);
            String separator = " WITH ";
            if (clustering != null)
            {
                redactedSchema = redactedSchema + separator + clustering;
                separator = " AND ";
            }

            List<String> propStrings = extractOverrideProperties(fullSchema, TABLE_PROPERTY_OVERRIDE_ALLOWLIST);
            if (!propStrings.isEmpty())
            {
                redactedSchema = redactedSchema + separator + String.join(" AND ", propStrings);
                separator = " AND ";  // For completeness
            }

            return redactedSchema + ";";
        }
        throw new RuntimeException(String.format("Could not find schema for table: %s.%s", keyspace, table));
    }

    @VisibleForTesting
    static List<String> extractOverrideProperties(String schemaStr, List<String> properties)
    {

        List<String> overrideTableProps = new ArrayList<>();
        if (properties.isEmpty())
        {
            return overrideTableProps;
        }
        Pattern pattern = Pattern.compile("(" + properties.stream().collect(Collectors.joining("|")) + ") = (([\\w|.]+)|(\\{[^}]+}))");
        Matcher matcher = pattern.matcher(schemaStr);

        while (matcher.find())
        {
            String parsedProp = schemaStr.substring(matcher.start(), matcher.end());
            overrideTableProps.add(parsedProp);
        }
        return overrideTableProps;
    }

    @VisibleForTesting
    static String extractClustering(String schemaStr)
    {
        Pattern pattern = Pattern.compile("CLUSTERING ORDER BY \\([^)]*");
        Matcher matcher = pattern.matcher(schemaStr);
        if (matcher.find())
        {
            return schemaStr.substring(matcher.start(0), matcher.end(0) + 1);
        }
        return null;
    }

    public static Set<String> extractUdts(@NotNull String schemaStr, @NotNull String keyspace)
    {
        Pattern pattern = Pattern.compile(String.format("CREATE TYPE (IF NOT EXISTS)? ?\"?%s\"?\\.{1}[^;]*;", keyspace));
        Matcher matcher = pattern.matcher(schemaStr);
        Set<String> result = new HashSet<>();
        while (matcher.find())
        {
            result.add(cleanCql(matcher.group()));
        }
        return result;
    }

    public static int extractIndexCount(@NotNull String schemaStr, @NotNull String keyspace, @NotNull String table)
    {
        return extractIndexStatements(schemaStr, keyspace, table).size();
    }

    /**
     * Extracts CREATE INDEX statements for the given table from the schema string.
     *
     * @param schemaStr full cluster schema text
     * @param keyspace  the keyspace name
     * @param table     the table name
     * @return set of CREATE INDEX statements for the table
     */
    public static Set<String> extractIndexStatements(@NotNull String schemaStr,
                                                     @NotNull String keyspace,
                                                     @NotNull String table)
    {
        String cleaned = cleanCql(schemaStr);
        // The (?!\w) after the table name is required: without it the table name matches as a prefix, so extracting
        // the indexes of "orders" would also pick up the CREATE INDEX statements of "orders_archive".
        Pattern pattern = Pattern.compile(String.format("CREATE (CUSTOM )?INDEX \"?[^ ]* ON ?\"?%s\"?\\.\"?%s\"?(?!\\w)[^;]*;",
                                                        Pattern.quote(keyspace), Pattern.quote(table)));
        Matcher matcher = pattern.matcher(cleaned);
        Set<String> statements = new HashSet<>();
        while (matcher.find())
        {
            // cleanCql strips newlines without substituting whitespace,
            // so collapse any resulting run of whitespace to a single space (and trim)
            // to keep the statement well-formed for the CQL parser.
            statements.add(MULTI_WHITESPACE_PATTERN.matcher(matcher.group()).replaceAll(" ").trim());
        }
        return statements;
    }

    /**
     * Returns true if the given CREATE INDEX statement defines a Storage Attached Index (SAI).
     * <p>
     * SAI class may appear as the short alias ("sai"), the short class name ("StorageAttachedIndex") or
     * fully qualified ("org.apache.cassandra.index.sai.StorageAttachedIndex"). All forms are case-insensitive,
     * matching Cassandra's own alias resolution (see {@code IndexMetadata.getIndexClassName}).
     *
     * @param createIndexStatement a CREATE INDEX CQL statement
     * @return true if the index uses SAI
     */
    public static boolean isSaiIndex(@NotNull String createIndexStatement)
    {
        // Matches any run of whitespace (spaces, tabs, newlines). Used to collapse a CREATE INDEX statement to
        // single-spaced text so the SAI marker can be matched regardless of how the schema text was formatted
        // (e.g. "USING  'StorageAttachedIndex'", a newline before USING, or a tab all normalize to one space).
        String normalized = MULTI_WHITESPACE_PATTERN.matcher(createIndexStatement).replaceAll(" ").toUpperCase(Locale.ROOT);

        // Matches the SAI marker inside a USING '...' clause (statement already upper-cased and whitespace-collapsed).
        // The 'SAI' alternative matches Cassandra's short alias (USING 'sai').
        // The optional canonical-package prefix (org.apache.cassandra.index.sai.) matches the fully-qualified class form,
        // and the bare STORAGEATTACHEDINDEX matches the short class name.
        // Restricting the prefix to the canonical SAI package (rather than any package) avoids false positives on a
        // foreign class that merely shares the simple name (e.g. 'foo.StorageAttachedIndex'), and the boundaries reject
        // names that merely end with the marker (e.g. 'PrefixStorageAttachedIndex'). These three forms are exactly what
        // Cassandra accepts and preserves in the schema.
        return SAI_USING_PATTERN.matcher(normalized).find();
    }

    /**
     * Returns true when {@code indexStatements} is non-empty and every statement defines a Storage Attached Index.
     *
     * @param indexStatements the CREATE INDEX statements for a table
     * @return true if all indexes are SAI (and at least one exists)
     */
    public static boolean hasOnlySaiIndexes(@NotNull Set<String> indexStatements)
    {
        return !indexStatements.isEmpty() && indexStatements.stream().allMatch(CqlUtils::isSaiIndex);
    }

    /**
     * Returns true when at least one statement in {@code indexStatements} defines a Storage Attached Index.
     * Unlike {@link #hasOnlySaiIndexes(Set)}, this tolerates a mix of SAI and legacy 2i indexes.
     *
     * @param indexStatements the CREATE INDEX statements for a table
     * @return true if at least one index is SAI
     */
    public static boolean hasAnySaiIndex(@NotNull Set<String> indexStatements)
    {
        return indexStatements.stream().anyMatch(CqlUtils::isSaiIndex);
    }

    /**
     * Extracts the compaction strategy used from table schema.
     *
     * @param tableSchema table schema
     * @return the compaction strategy, or null if not found
     */
    public static String extractCompactionStrategy(@NotNull String tableSchema)
    {
        Matcher matcher = COMPACTION_STRATEGY_PATTERN.matcher(tableSchema);
        if (matcher.find())
        {
            return matcher.group(1);
        }
        return null;
    }

    /**
     * Time range filter is only supported for TimeWindowCompactionStrategy.
     *
     * @return true if the strategy is TimeWindowCompactionStrategy, false otherwise
     */
    public static boolean isTimeRangeFilterSupported(String compactionStrategy)
    {
        return compactionStrategy == null || compactionStrategy.endsWith("TimeWindowCompactionStrategy");
    }
}
