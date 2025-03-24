<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# native_types Example

## Cassandra Schema

```
CREATE TABLE examples.native_types (
    pk text PRIMARY KEY,
    a_tinyint tinyint,
    a_smallint smallint,
    an_int int,
    a_long bigint,
    a_uuid uuid,
    a_timeuuid timeuuid,
    a_float float,
    a_double double,
    a_blob blob,
    an_inet inet,
    a_bool boolean,
    a_string text,
    an_ascii ascii,
    a_varchar varchar,
    a_date date,
    a_timestamp timestamp,
    a_time time,
    a_big_integer varint
)
```

## INSERT Example

An INSERT into all primary keys and value columns.

### Avro Header

```
{
  "timestampMicros" : 1711149035727000,
  "sourceTable" : "native_types",
  "sourceKeyspace" : "examples",
  "schemaUuid" : "9b1faa02-2fc9-390d-a5b5-be7190fee276",
  "truncatedFields" : [ ],
  "version" : {
    "string" : "2"
  },
  "operationType" : "INSERT",
  "isPartial" : true,
  "updateFields" : {
    "array" : [ "pk", "a_big_integer", "a_blob", "a_bool", "a_date", "a_double", "a_float", "a_long", "a_smallint", "a_string", "a_time", "a_timestamp", "a_timeuuid", "a_tinyint", "a_uuid", "a_varchar", "an_ascii", "an_inet", "an_int" ]
  },
  "range" : null,
  "ttl" : null,
  "payload": "[binary serialized value of payload below]"
}
```



### Avro Payload

```
{
  "pk" : {
    "string" : "wR02tKI1xDe"
  },
  "a_big_integer" : {
    "fixed" : "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u00000ÄHß"
  },
  "a_blob" : {
    "bytes" : ",\u0015p\u000Eüâ¶7¦ÙRëÅü1A@?<ä1(\u0010ê\u0000\u0018\u001FY\f³\u000E°|tf}°Óo9ñB{¼qóUé·<cOÎlI>Y\t¾ÈÅ³Çyúu¿¢m¥úfüÛ:QCÿ\r°\u001ArZÔ«\u000FÂÁµñ]/ÅR[ú¨Ù\u0012íàÑå?\u0012ZµëÂ\t9ÝzR\u0000.VçM}©¸¨ë ð!ÖÍd\u0012=©KYê£}\u001A\u0007ÊFÆj×"
  },
  "a_bool" : {
    "boolean" : false
  },
  "a_date" : {
    "int" : 359
  },
  "a_double" : {
    "double" : 0.9669303482909445
  },
  "a_float" : {
    "float" : 0.8431084
  },
  "a_long" : {
    "long" : 4402533
  },
  "a_smallint" : {
    "int" : 26250
  },
  "a_string" : {
    "string" : "8AhcJcXRZ6aNW6QhNSqzsz"
  },
  "a_time" : {
    "long" : 2623283
  },
  "a_timestamp" : {
    "long" : 1711149035715000
  },
  "a_timeuuid" : {
    "string" : "63d20130-e8a1-11ee-b0ad-533b4b1bc010"
  },
  "a_tinyint" : {
    "int" : -97
  },
  "a_uuid" : {
    "string" : "7a6c8480-cef3-4e32-ab38-a82c7d8967a9"
  },
  "a_varchar" : {
    "string" : "k9XvOSLP354zqc9J1h3OnNm"
  },
  "an_ascii" : {
    "string" : "3pgcDT9y8tZloMhvO5P01B"
  },
  "an_inet" : {
    "bytes" : "Ïø8"
  },
  "an_int" : {
    "int" : -284872511
  }
}
```

## UPDATE Example

An UPDATE into all primary keys and a single value column.

### Avro Header

```
{
  "timestampMicros" : 1711149035731000,
  "sourceTable" : "native_types",
  "sourceKeyspace" : "examples",
  "schemaUuid" : "9b1faa02-2fc9-390d-a5b5-be7190fee276",
  "truncatedFields" : [ ],
  "version" : {
    "string" : "2"
  },
  "operationType" : "UPDATE",
  "isPartial" : true,
  "updateFields" : {
    "array" : [ "pk", "a_big_integer" ]
  },
  "range" : null,
  "ttl" : null,
  "payload": "[binary serialized value of payload below]"
}
```



### Avro Payload

```
{
  "pk" : {
    "string" : "BAbjNRSNk42dbVNUWl"
  },
  "a_big_integer" : {
    "fixed" : "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000<Îúô"
  },
  "a_blob" : null,
  "a_bool" : null,
  "a_date" : null,
  "a_double" : null,
  "a_float" : null,
  "a_long" : null,
  "a_smallint" : null,
  "a_string" : null,
  "a_time" : null,
  "a_timestamp" : null,
  "a_timeuuid" : null,
  "a_tinyint" : null,
  "a_uuid" : null,
  "a_varchar" : null,
  "an_ascii" : null,
  "an_inet" : null,
  "an_int" : null
}
```

## DELETE Example

A point DELETE of a single value column.

### Avro Header

```
{
  "timestampMicros" : 1711149035732000,
  "sourceTable" : "native_types",
  "sourceKeyspace" : "examples",
  "schemaUuid" : "9b1faa02-2fc9-390d-a5b5-be7190fee276",
  "truncatedFields" : [ ],
  "version" : {
    "string" : "2"
  },
  "operationType" : "DELETE",
  "isPartial" : true,
  "updateFields" : {
    "array" : [ "pk", "a_big_integer" ]
  },
  "range" : null,
  "ttl" : null,
  "payload": "[binary serialized value of payload below]"
}
```



### Avro Payload

```
{
  "pk" : {
    "string" : "dTmn"
  },
  "a_big_integer" : null,
  "a_blob" : null,
  "a_bool" : null,
  "a_date" : null,
  "a_double" : null,
  "a_float" : null,
  "a_long" : null,
  "a_smallint" : null,
  "a_string" : null,
  "a_time" : null,
  "a_timestamp" : null,
  "a_timeuuid" : null,
  "a_tinyint" : null,
  "a_uuid" : null,
  "a_varchar" : null,
  "an_ascii" : null,
  "an_inet" : null,
  "an_int" : null
}
```

## PARTITION_DELETE Example

A PARTITION_DELETE that deletes using only the partition keys.

### Avro Header

```
{
  "timestampMicros" : 1711149035733000,
  "sourceTable" : "native_types",
  "sourceKeyspace" : "examples",
  "schemaUuid" : "9b1faa02-2fc9-390d-a5b5-be7190fee276",
  "truncatedFields" : [ ],
  "version" : {
    "string" : "2"
  },
  "operationType" : "DELETE_PARTITION",
  "isPartial" : true,
  "updateFields" : {
    "array" : [ "pk" ]
  },
  "range" : null,
  "ttl" : null,
  "payload": "[binary serialized value of payload below]"
}
```



### Avro Payload

```
{
  "pk" : {
    "string" : "0"
  },
  "a_big_integer" : null,
  "a_blob" : null,
  "a_bool" : null,
  "a_date" : null,
  "a_double" : null,
  "a_float" : null,
  "a_long" : null,
  "a_smallint" : null,
  "a_string" : null,
  "a_time" : null,
  "a_timestamp" : null,
  "a_timeuuid" : null,
  "a_tinyint" : null,
  "a_uuid" : null,
  "a_varchar" : null,
  "an_ascii" : null,
  "an_inet" : null,
  "an_int" : null
}
```

