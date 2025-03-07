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

# Clusteringkeys Example

## Cassandra Schema

```
CREATE TABLE examples.clusteringkeys (
    a text,
    b int,
    c text,
    d int,
    e boolean,
    f uuid,
    g timestamp,
    PRIMARY KEY((a, b), c, d)
)
```

## INSERT Example

An INSERT into all primary keys and value columns.

### Avro Header

```
{
  "timestampMicros" : 1711149035697000,
  "sourceTable" : "clusteringkeys",
  "sourceKeyspace" : "examples",
  "schemaUuid" : "d31bc632-222c-3efb-9200-6678584d7655",
  "truncatedFields" : [ ],
  "version" : {
    "string" : "2"
  },
  "operationType" : "INSERT",
  "isPartial" : true,
  "updateFields" : {
    "array" : [ "a", "b", "c", "d", "e", "f", "g" ]
  },
  "range" : null,
  "ttl" : null,
  "payload": "[binary serialized value of payload below]"
}
```



### Avro Payload

```
{
  "a" : {
    "string" : "7pJj8tRup"
  },
  "b" : {
    "int" : 1395469866
  },
  "c" : {
    "string" : "y"
  },
  "d" : {
    "int" : -1769672389
  },
  "e" : {
    "boolean" : false
  },
  "f" : {
    "string" : "3bfbb07e-fe24-47dd-9fed-f7d2af38f970"
  },
  "g" : {
    "long" : 1711149035691000
  }
}
```

## UPDATE Example

An UPDATE into all primary keys and a single value column.

### Avro Header

```
{
  "timestampMicros" : 1711149035706000,
  "sourceTable" : "clusteringkeys",
  "sourceKeyspace" : "examples",
  "schemaUuid" : "d31bc632-222c-3efb-9200-6678584d7655",
  "truncatedFields" : [ ],
  "version" : {
    "string" : "2"
  },
  "operationType" : "UPDATE",
  "isPartial" : true,
  "updateFields" : {
    "array" : [ "a", "b", "c", "d", "e" ]
  },
  "range" : null,
  "ttl" : null,
  "payload": "[binary serialized value of payload below]"
}
```



### Avro Payload

```
{
  "a" : {
    "string" : "mtj16WKgwYWl3g"
  },
  "b" : {
    "int" : 1087728539
  },
  "c" : {
    "string" : "fVVyYNPv9"
  },
  "d" : {
    "int" : -48936319
  },
  "e" : {
    "boolean" : false
  },
  "f" : null,
  "g" : null
}
```

## DELETE Example

A point DELETE of a single value column.

### Avro Header

```
{
  "timestampMicros" : 1711149035707000,
  "sourceTable" : "clusteringkeys",
  "sourceKeyspace" : "examples",
  "schemaUuid" : "d31bc632-222c-3efb-9200-6678584d7655",
  "truncatedFields" : [ ],
  "version" : {
    "string" : "2"
  },
  "operationType" : "DELETE",
  "isPartial" : true,
  "updateFields" : {
    "array" : [ "a", "b", "c", "d", "e" ]
  },
  "range" : null,
  "ttl" : null,
  "payload": "[binary serialized value of payload below]"
}
```



### Avro Payload

```
{
  "a" : {
    "string" : "XwebOcUlooA0n1DhDQRFNKuBZ92mx"
  },
  "b" : {
    "int" : 1517466481
  },
  "c" : {
    "string" : "cmDnUPhtLJcDFbFdshzjJeF"
  },
  "d" : {
    "int" : 1443709363
  },
  "e" : null,
  "f" : null,
  "g" : null
}
```

## PARTITION_DELETE Example

A PARTITION_DELETE that deletes using only the partition keys.

### Avro Header

```
{
  "timestampMicros" : 1711149035707000,
  "sourceTable" : "clusteringkeys",
  "sourceKeyspace" : "examples",
  "schemaUuid" : "d31bc632-222c-3efb-9200-6678584d7655",
  "truncatedFields" : [ ],
  "version" : {
    "string" : "2"
  },
  "operationType" : "DELETE_PARTITION",
  "isPartial" : true,
  "updateFields" : {
    "array" : [ "a", "b" ]
  },
  "range" : null,
  "ttl" : null,
  "payload": "[binary serialized value of payload below]"
}
```



### Avro Payload

```
{
  "a" : {
    "string" : "oszbajenC"
  },
  "b" : {
    "int" : -1905779150
  },
  "c" : null,
  "d" : null,
  "e" : null,
  "f" : null,
  "g" : null
}
```

## ROW_DELETE Example

A ROW_DELETE that deletes using only the partition keys and clustering keys.

### Avro Header

```
{
  "timestampMicros" : 1711149035708000,
  "sourceTable" : "clusteringkeys",
  "sourceKeyspace" : "examples",
  "schemaUuid" : "d31bc632-222c-3efb-9200-6678584d7655",
  "truncatedFields" : [ ],
  "version" : {
    "string" : "2"
  },
  "operationType" : "DELETE",
  "isPartial" : true,
  "updateFields" : {
    "array" : [ "a", "b", "c", "d" ]
  },
  "range" : null,
  "ttl" : null,
  "payload": "[binary serialized value of payload below]"
}
```



### Avro Payload

```
{
  "a" : {
    "string" : "Saq0gG9n3tIQH4fqqbSWrdmfr6WJ"
  },
  "b" : {
    "int" : -247106775
  },
  "c" : {
    "string" : "Svt9BI67iu43gKEH0Trm4uBHw"
  },
  "d" : {
    "int" : -2135516922
  },
  "e" : null,
  "f" : null,
  "g" : null
}
```

## RANGE_DELETE Example

A RANGE_DELETE that deletes using only the partition keys and a range on a clustering key.

### Avro Header

```
{
  "timestampMicros" : 1711149035708000,
  "sourceTable" : "clusteringkeys",
  "sourceKeyspace" : "examples",
  "schemaUuid" : "d31bc632-222c-3efb-9200-6678584d7655",
  "truncatedFields" : [ ],
  "version" : {
    "string" : "2"
  },
  "operationType" : "DELETE_RANGE",
  "isPartial" : true,
  "updateFields" : {
    "array" : [ "a", "b" ]
  },
  "range" : {
    "array" : [ {
      "field" : "c",
      "rangePredicateType" : "GT",
      "value" : "\u0002\u0002\u0000<0un8cCpqbM7nnG5kChtHlm0wl7qAoL\u0002\u0002\u0002\u0002"
    }, {
      "field" : "c",
      "rangePredicateType" : "LTE",
      "value" : "\u0002\u0002\u0000,0UbRvwVua9ucGMCmAhZ7In\u0002\u0002\u0002\u0002"
    } ]
  },
  "ttl" : null,
  "payload": "[binary serialized value of payload below]"
}
```



### Avro Payload

```
{
  "a" : {
    "string" : "sYg"
  },
  "b" : {
    "int" : 2124310699
  },
  "c" : null,
  "d" : null,
  "e" : null,
  "f" : null,
  "g" : null
}
```

