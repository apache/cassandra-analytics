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


# basic Example

## Cassandra Schema

```
CREATE TABLE examples.basic (
    a text PRIMARY KEY,
    b text,
    c text
)
```

## INSERT Example

An INSERT into all primary keys and value columns.

### Avro Header

```
{
  "timestampMicros" : 1711149035712000,
  "sourceTable" : "basic",
  "sourceKeyspace" : "examples",
  "schemaUuid" : "c02c81f8-83fb-3c36-988e-1cca7691e814",
  "truncatedFields" : [ ],
  "version" : {
    "string" : "2"
  },
  "operationType" : "INSERT",
  "isPartial" : true,
  "updateFields" : {
    "array" : [ "a", "b", "c" ]
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
    "string" : "QsbX46DoBcpKOII2ggfRuP"
  },
  "b" : {
    "string" : "fs"
  },
  "c" : {
    "string" : "h4DCyo7N6h6mRIfQjX0Y"
  }
}
```

## UPDATE Example

An UPDATE into all primary keys and a single value column.

### Avro Header

```
{
  "timestampMicros" : 1711149035712000,
  "sourceTable" : "basic",
  "sourceKeyspace" : "examples",
  "schemaUuid" : "c02c81f8-83fb-3c36-988e-1cca7691e814",
  "truncatedFields" : [ ],
  "version" : {
    "string" : "2"
  },
  "operationType" : "UPDATE",
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
    "string" : "rnh5W9"
  },
  "b" : {
    "string" : "hDmOY7Z4giTUAWuAk2vufwIYjj"
  },
  "c" : null
}
```

## DELETE Example

A point DELETE of a single value column.

### Avro Header

```
{
  "timestampMicros" : 1711149035712000,
  "sourceTable" : "basic",
  "sourceKeyspace" : "examples",
  "schemaUuid" : "c02c81f8-83fb-3c36-988e-1cca7691e814",
  "truncatedFields" : [ ],
  "version" : {
    "string" : "2"
  },
  "operationType" : "DELETE",
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
    "string" : "Asemaitw9"
  },
  "b" : null,
  "c" : null
}
```

## PARTITION_DELETE Example

A PARTITION_DELETE that deletes using only the partition keys.

### Avro Header

```
{
  "timestampMicros" : 1711149035713000,
  "sourceTable" : "basic",
  "sourceKeyspace" : "examples",
  "schemaUuid" : "c02c81f8-83fb-3c36-988e-1cca7691e814",
  "truncatedFields" : [ ],
  "version" : {
    "string" : "2"
  },
  "operationType" : "DELETE_PARTITION",
  "isPartial" : true,
  "updateFields" : {
    "array" : [ "a" ]
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
    "string" : "jqNMVJDtBLfxWmX1SC"
  },
  "b" : null,
  "c" : null
}
```

