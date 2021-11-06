# Kinesis Glue

Implementation of the concepts presented in [this](https://aws.amazon.com/blogs/big-data/validate-evolve-and-control-schemas-in-amazon-msk-and-amazon-kinesis-data-streams-with-aws-glue-schema-registry/) blog entry.

Note schemas are created on the fly if they do not exist:

Before:

``
aws glue list-schemas        
{
"Schemas": []
}
``

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
            "RegistryName": "default-registry",
            "SchemaName": "quoteSchema",
            "SchemaArn": "arn:aws:glue:us-east-1:nnnnnnnnnn:schema/default-registry/quoteSchema",
            "Description": "DEFAULT-DESCRIPTION-us-east-1-default-registry",
            "SchemaStatus": "AVAILABLE",
            "CreatedTime": "2021-11-06T16:30:02.089Z",
            "UpdatedTime": "2021-11-06T16:30:02.089Z"
        }
    ]
}
```
