# Sample - JSON Schema and Schema Registry

First time we run it...

```
15:30:31,374 INFO  org.ds.flink.kinesisglue.json.producer.sample1.Sample        [] - get the schema from the registry
15:30:31,417 INFO  org.ds.flink.kinesisglue.json.producer.sample1.Sample        [] - gsrSchema is Schema(schemaDefinition={"$schema":"http://json-schema.org/draft-04/schema#","type":"object","properties":{"symbol":{"type":"string"},"price":{"type":"number"}},"required":["symbol","price"]}, dataFormat=JSON, schemaName=JsonQuote2)
15:30:31,418 INFO  org.ds.flink.kinesisglue.json.producer.sample1.Sample        [] - create serialized bytes from record
15:30:31,513 INFO  org.ds.flink.kinesisglue.json.producer.sample1.Sample        [] - encode bytes using gsr serializer
15:30:32,619 INFO  com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient [] - Auto Creating schema with schemaName: JsonQuote2 and schemaDefinition : {"$schema":"http://json-schema.org/draft-04/schema#","type":"object","properties":{"symbol":{"type":"string"},"price":{"type":"number"}},"required":["symbol","price"]}
15:30:34,025 INFO  org.ds.flink.kinesisglue.json.producer.sample1.Sample        [] - {"$schema":"http://json-schema.org/draft-04/schema#","type":"object","properties":{"symbol":{"type":"string"},"price":{"type":"number"}},"required":["symbol","price"]}
15:30:34,034 INFO  org.ds.flink.kinesisglue.json.producer.sample1.Sample        [] - JsonDataWithSchema(schema={"$schema":"http://json-schema.org/draft-04/schema#","type":"object","properties":{"symbol":{"type":"string"},"price":{"type":"number"}},"required":["symbol","price"]}, payload={"symbol":"AAPL","price":12312.23})
15:30:34,034 INFO  org.ds.flink.kinesisglue.json.producer.sample1.Sample        [] - done...
```

Second time...

```
15:31:16,126 INFO  org.ds.flink.kinesisglue.json.producer.sample1.Sample        [] - get the schema from the registry
15:31:16,158 INFO  org.ds.flink.kinesisglue.json.producer.sample1.Sample        [] - gsrSchema is Schema(schemaDefinition={"$schema":"http://json-schema.org/draft-04/schema#","type":"object","properties":{"symbol":{"type":"string"},"price":{"type":"number"}},"required":["symbol","price"]}, dataFormat=JSON, schemaName=JsonQuote2)
15:31:16,158 INFO  org.ds.flink.kinesisglue.json.producer.sample1.Sample        [] - create serialized bytes from record
15:31:16,260 INFO  org.ds.flink.kinesisglue.json.producer.sample1.Sample        [] - encode bytes using gsr serializer
15:31:17,793 INFO  org.ds.flink.kinesisglue.json.producer.sample1.Sample        [] - {"$schema":"http://json-schema.org/draft-04/schema#","type":"object","properties":{"symbol":{"type":"string"},"price":{"type":"number"}},"required":["symbol","price"]}
15:31:17,798 INFO  org.ds.flink.kinesisglue.json.producer.sample1.Sample        [] - JsonDataWithSchema(schema={"$schema":"http://json-schema.org/draft-04/schema#","type":"object","properties":{"symbol":{"type":"string"},"price":{"type":"number"}},"required":["symbol","price"]}, payload={"symbol":"AAPL","price":12312.23})
15:31:17,799 INFO  org.ds.flink.kinesisglue.json.producer.sample1.Sample        [] - done...
```