### Description
Pulsar is a high-performance, persistent message middleware that supports multiple programming languages ​​and data models. This article will focus on the comparison between Pulsar's GO Client and JAVA Client in Schema compatibility. We'll focus on four common schema types: Avro, JSON, Proto, and Proto native schema.
#### Avro Schema
Avro schema in Go and Java are compatible, but there are some differences in how the schemas are defined. Go typically uses schema definitions, a string Json,  to create schemas, whereas Java often uses class types for schema creation. As a result, Java allows non-primitive fields to be nullable by default, while in Go's schema definition, the nullability of fields needs to be explicitly stated.
GO:
```go
// Compatible with Java schema define
exampleSchemaDefCompatible := NewAvroSchema(`{"fields":
    [
        {"name":"id","type":"int"},{"default":null,"name":"name","type":["null","string"]}
    ],
    "name":"MyAvro","namespace":"schemaNotFoundTestCase","type":"record"}`, nil)
// Not compatible with Java schema define
exampleSchemaDefIncompatible := NewAvroSchema(`{"fields":
    [
        {"name":"id","type":"int"},{"default":null,"name":"name","type":["string"]}
    ],
    "name":"MyAvro","namespace":"schemaNotFoundTestCase","type":"record"}`, nil)    
Producer := NewAvroSchema(exampleSchemaDef, nil)

```
JAVA:
```java
@AllArgsConstructor
@NoArgsConstructor
public static class Example {
    public String name;
    public int id;
}

Producer<Example> producer = pulsarClient.newProducer(Schema.AVRO(Example.class))
                .topic(topic).create();
```

#### JSON Schema
The situation with JSON schema is similar to Avro Schema.
```go
// Compatible with Java schema define
exampleSchemaDefCompatible := "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
	"\"fields\":[{\"name\":\"ID\",\"type\":\"int\"},{\"name\":\"Name\",\"type\":[\"null\", \"string\"]}]}"

consumerJSCompatible := NewJSONSchema(exampleSchemaDefCompatible, nil)
// Not compatible with Java schema define
exampleSchemaDefIncompatible := "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
	"\"fields\":[{\"name\":\"ID\",\"type\":\"int\"},{\"name\":\"Name\",\"type\":\"string\"}]}"

consumerJSIncompatible := NewJSONSchema(exampleSchemaDefIncompatible, nil)
```

To achieve compatibility, modify the `exampleSchemaDefIncompatible` to allow null fields and ensure that the variable names in the Java Example class match the schema definition.

#### Proto Schema
Proto and ProtoNative schemas exhibit some incompatibility between Go and Java clients. This is because Avro Proto currently does not provide full compatibility between Java and Go.

```proto
message TestMessage {
    string stringField = 1;
    int32 intField = 2;
}
```

Schema define in java，which can be parsed by a Class.
```json
protoSchemaDef = "{\"type\":\"record\",\"name\":\"TestMessage\",\"namespace\":\"org.apache.pulsar.client.api.schema.proto.Test\",\"fields\":[{\"name\":\"stringField\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"default\":\"\"},{\"name\":\"intField\",\"type\":\"int\",\"default\":0}]}"

```

Schema define in GO, which need to be manually written.
```json
protoSchemaDef = "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
		"\"fields\":[{\"name\":\"num\",\"type\":\"int\"},{\"name\":\"msf\",\"type\":\"string\"}]}"
```
To address the incompatibility between Proto and ProtoNative types, you can follow this approach:
1. In the Java client, parse the message using the Avro Proto library to obtain the Schema Definition.
2. Use this obtained schema definition in the Go client to ensure both clients use the same schema definition.
```json
protoSchemaDef = "{\"type\":\"record\",\"name\":\"TestMessage\",\"namespace\":\"org.apache.pulsar.client.api.schema.proto.Test\",\"fields\":[{\"name\":\"stringField\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"default\":\"\"},{\"name\":\"intField\",\"type\":\"int\",\"default\":0}]}"

```
3. Modify the Go Proto Message by adding compatibility extensions. For example, add `[(avro_java_string) = "String"]` extension to string type fields.
```proto
message TestMessage {
    string stringField = 1 [(avro_java_string) = "String"];
    int32 intField = 2;
}
```

#### ProtoNative Schema
Similar to the Proto schema, ProtoNative schemas are also incompatible between Java and Go clients. To address this, you can use a unified schema define and add `[(avro_java_string) = "String"]` extension to the Go client's Proto message.