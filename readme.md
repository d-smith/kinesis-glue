# Kinesis Glue

Implementation of the concepts presented in [this](https://aws.amazon.com/blogs/big-data/validate-evolve-and-control-schemas-in-amazon-msk-and-amazon-kinesis-data-streams-with-aws-glue-schema-registry/) blog entry.

Note schema registries are created on the fly and schemas added if they do not exist:

Before:

```
aws glue list-schemas        
{
"Schemas": []
}
```

Create the registry

```
aws glue create-registry --registry-name registry-sample
```

Create the stream...

```
 aws kinesis create-stream --stream-name kpltest --shard-count 1
```

Run the producer... now list the schemas.

```
 % aws glue list-schemas   
{
    "Schemas": [
        {
            "RegistryName": "registry-sample",
            "SchemaName": "quoteSchema",
            "SchemaArn": "arn:aws:glue:us-east-1:nnnnnnnnn:schema/registry-sample/quoteSchema",
            "Description": "registry to store schema for sample appllication",
            "SchemaStatus": "AVAILABLE",
            "CreatedTime": "2021-11-07T16:42:28.617Z",
            "UpdatedTime": "2021-11-07T16:42:28.617Z"
        }
    ]
}

```

To get the schema definition

aws glue get-schema --schema-id SchemaName=quoteSchema,RegistryName=registry-sample

```
{
    "RegistryName": "registry-sample",
    "RegistryArn": "arn:aws:glue:us-east-1:230586709464:registry/registry-sample",
    "SchemaName": "quoteSchema",
    "SchemaArn": "arn:aws:glue:us-east-1:230586709464:schema/registry-sample/quoteSchema",
    "Description": "registry to store schema for sample appllication",
    "DataFormat": "AVRO",
    "Compatibility": "BACKWARD",
    "SchemaCheckpoint": 1,
    "LatestSchemaVersion": 1,
    "NextSchemaVersion": 2,
    "SchemaStatus": "AVAILABLE",
    "CreatedTime": "2021-11-07T16:42:28.617Z",
    "UpdatedTime": "2021-11-07T16:42:28.617Z"
}
```


