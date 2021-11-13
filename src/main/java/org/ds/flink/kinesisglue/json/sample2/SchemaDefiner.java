package org.ds.flink.kinesisglue.json.sample2;

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatSerializer;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerFactory;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.util.HashMap;

public class SchemaDefiner {
    private static Logger logger = LoggerFactory.getLogger(SchemaDefiner.class);

    public static void main(String... args) {
        String jsonSchema = "{" +
                "\"$schema\": \"http://json-schema.org/draft-04/schema#\"," +
                "\"type\":\"object\"," +
                "\"properties\": {" +
                "\"symbol\":{\"type\":\"string\"}," +
                "\"price\":{\"type\":\"number\"}" +
                "}," +
                "\"required\":[\"symbol\",\"price\"]}";


        //Configurations for Schema Registry
        GlueSchemaRegistryConfiguration gsrConfig =
                new GlueSchemaRegistryConfiguration(System.getenv("AWS_REGION"));
        gsrConfig.setSchemaAutoRegistrationEnabled(true);
        gsrConfig.setRegistryName("registry-sample");
        gsrConfig.setDescription("registry to store schema for sample appllication");
        gsrConfig.setCacheSize(100);
        gsrConfig.setTimeToLiveMillis(24*60*60*1000);

        AwsCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.builder()
                .build();

        logger.info("create schema");




        AWSSchemaRegistryClient schemaRegistryClient =
                new AWSSchemaRegistryClient(awsCredentialsProvider, gsrConfig);
        schemaRegistryClient.createSchema("JsonQuoteIII",DataFormat.JSON.name(),
                jsonSchema,new HashMap<>());
        logger.info("done");
    }
}
