# Sample - JSON Schema and Schema Registry

First time we run it...

```
14:58:03,420 INFO  org.ds.flink.kinesisglue.json.producer.Sample                [] - get the schema from the registry
14:58:03,465 INFO  org.ds.flink.kinesisglue.json.producer.Sample                [] - read Schema(schemaDefinition={"$schema":"http://json-schema.org/draft-04/schema#","type":"object","properties":{"symbol":{"type":"string"},"price":{"type":"number"}},"required":["symbol","price"]}, dataFormat=JSON, schemaName=JsonQuote2)
14:58:04,682 INFO  com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient [] - Auto Creating schema with schemaName: JsonQuote2 and schemaDefinition : {"$schema":"http://json-schema.org/draft-04/schema#","type":"object","properties":{"symbol":{"type":"string"},"price":{"type":"number"}},"required":["symbol","price"]}
14:58:06,076 INFO  org.ds.flink.kinesisglue.json.producer.Sample                [] - {"$schema":"http://json-schema.org/draft-04/schema#","type":"object","properties":{"symbol":{"type":"string"},"price":{"type":"number"}},"required":["symbol","price"]}
14:58:06,087 INFO  org.ds.flink.kinesisglue.json.producer.Sample                [] - JsonDataWithSchema(schema={"$schema":"http://json-schema.org/draft-04/schema#","type":"object","properties":{"symbol":{"type":"string"},"price":{"type":"number"}},"required":["symbol","price"]}, payload={"symbol":"AAPL","price":12312.23})
14:58:06,087 INFO  org.ds.flink.kinesisglue.json.producer.Sample                [] - done...
```

Second time...

```
5:00:34,695 INFO  org.ds.flink.kinesisglue.json.producer.Sample                [] - get the schema from the registry
15:00:34,736 INFO  org.ds.flink.kinesisglue.json.producer.Sample                [] - read Schema(schemaDefinition={"$schema":"http://json-schema.org/draft-04/schema#","type":"object","properties":{"symbol":{"type":"string"},"price":{"type":"number"}},"required":["symbol","price"]}, dataFormat=JSON, schemaName=JsonQuote2)
15:00:36,795 INFO  org.ds.flink.kinesisglue.json.producer.Sample                [] - {"$schema":"http://json-schema.org/draft-04/schema#","type":"object","properties":{"symbol":{"type":"string"},"price":{"type":"number"}},"required":["symbol","price"]}
15:00:36,803 INFO  org.ds.flink.kinesisglue.json.producer.Sample                [] - JsonDataWithSchema(schema={"$schema":"http://json-schema.org/draft-04/schema#","type":"object","properties":{"symbol":{"type":"string"},"price":{"type":"number"}},"required":["symbol","price"]}, payload={"symbol":"AAPL","price":12312.23})
15:00:36,803 INFO  org.ds.flink.kinesisglue.json.producer.Sample                [] - done...
```