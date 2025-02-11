package org.apache.cassandra.cdc.schemastore;


import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;


/**
 * This is an example implementation of a Schema Store. In this particular implementation, we rely on avro schemas
 * placed under the resources folder, inside the table_schemas folder. They will be used as schemas
 * for the tables that have CDC enabled.
 */
public class LocalTableSchemaStore implements SchemaStore
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalTableSchemaStore.class);
    private final Map<String, Schema> cache = new HashMap<>();

    private final Map<String, GenericDatumWriter<GenericRecord>> writers = new HashMap<>();
    private final Map<String, GenericDatumReader<GenericRecord>> readers = new HashMap<>();

    private static class Holder
    {
        private static final LocalTableSchemaStore INSTANCE = new LocalTableSchemaStore();
    }

    public static LocalTableSchemaStore getInstance()
    {
        return Holder.INSTANCE;
    }

    protected LocalTableSchemaStore()
    {
        // must register the custom logical types before loading the avro schemas
        loadFromResource();
    }

    /**
     * Expects cassandra keyspace name for namespace and cassandra table name for name.
     * @return the schema, or throws
     */
    @Override
    public Schema getSchema(String namespace, String name)
    {
        return cache.get(namespace);
    }

    /**
     * @return the writer, or throws if schema is not found
     */
    @Override
    public GenericDatumWriter<GenericRecord> getWriter(String namespace, String name)
    {
        return writers.get(namespace);
    }

    /**
     * @return the reader, or throws if schema is not found
     */
    @Override
    public GenericDatumReader<GenericRecord> getReader(String namespace, String name)
    {
        return readers.get(namespace);
    }

    protected void loadFromResource()
    {
        FileSystem jarFs = null;
        try
        {
            URL url = getClass().getClassLoader().getResource("table_schemas");
            if (url == null)
            {
                throw new RuntimeException("Resource table_schemas not found");
            }

            URI schemas = url.toURI();
            Path path;
            if (schemas.getScheme().equals("jar"))
            {
                jarFs = FileSystems.newFileSystem(schemas, Map.of());
                path = jarFs.getPath("table_schemas");
            }
            else
            {
                path = Paths.get(schemas);
            }

            try (Stream<Path> paths = Files.walk(path, 1))
            {
                Schema.Parser parser = new Schema.Parser();
                paths.forEach(p -> {
                    if (!p.toString().endsWith(".avsc"))
                    {
                        return;
                    }
                    try
                    {
                        InputStream is = Files.newInputStream(p);
                        Schema schema = parser.parse(is);
                        String key = schema.getNamespace();
                        LOGGER.info("Loading schema namespace={}", key);
                        cache.put(key, schema);
                        writers.put(key, new GenericDatumWriter<>(schema));
                        readers.put(key, new GenericDatumReader<>(schema));
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
        catch (URISyntaxException e)
        {
            throw new RuntimeException(e);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to read from table_schemas", e);
        }
        finally
        {
            if (jarFs != null)
            {
                try
                {
                    jarFs.close();
                }
                catch (IOException e)
                {
                    throw new RuntimeException("Unable to close jar", e);
                }
            }
        }
    }
}
