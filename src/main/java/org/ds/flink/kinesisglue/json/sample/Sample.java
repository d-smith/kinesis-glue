package org.ds.flink.kinesisglue.json.sample;

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatDeserializer;
import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatSerializer;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializer;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerFactory;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerFactory;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.nio.ByteBuffer;

public class Sample {
    private static  Logger logger = LoggerFactory.getLogger(Sample.class);

    public static void main(String... args) {

        String jsonSchema = "{" +
                "\"$schema\": \"http://json-schema.org/draft-04/schema#\"," +
                "\"type\":\"object\"," +
                "\"properties\": {" +
                "\"symbol\":{\"type\":\"string\"}," +
                "\"price\":{\"type\":\"number\"}" +
                "}," +
                "\"required\":[\"symbol\",\"price\"]}";


        String jsonPayload = "{\"symbol\":\"AAPL\",\"price\":12312.23}";
        JsonDataWithSchema jsonSchemaWithData = JsonDataWithSchema.builder(jsonSchema, jsonPayload).build();
        logger.info(jsonSchemaWithData.toString());

        JsonDataWithSchema record = JsonDataWithSchema.builder(jsonSchema, jsonPayload).build();
        DataFormat dataFormat = DataFormat.JSON;


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


        GlueSchemaRegistrySerializer glueSchemaRegistrySerializer =
                new GlueSchemaRegistrySerializerImpl(awsCredentialsProvider, gsrConfig);
        GlueSchemaRegistryDataFormatSerializer dataFormatSerializer =
                new GlueSchemaRegistrySerializerFactory().getInstance(dataFormat, gsrConfig);

        logger.info("get the schema from the registry");
        Schema gsrSchema =
                new Schema(dataFormatSerializer.getSchemaDefinition(record), dataFormat.name(), "JsonQuote2");
        logger.info("gsrSchema is {}", gsrSchema.toString());

        logger.info("create serialized bytes from record");
        byte[] serializedBytes = dataFormatSerializer.serialize(record);

        logger.info("encode bytes using gsr serializer");
        byte[] gsrEncodedBytes = glueSchemaRegistrySerializer.encode("kpltest", gsrSchema, serializedBytes);

        GlueSchemaRegistryDeserializer gsrDeserializer =
                new GlueSchemaRegistryDeserializerImpl(awsCredentialsProvider, gsrConfig);
        GlueSchemaRegistryDataFormatDeserializer dataFormatDeserializer =
                new GlueSchemaRegistryDeserializerFactory().getInstance(dataFormat, gsrConfig);

        Schema theSchema = gsrDeserializer.getSchema(gsrEncodedBytes);
        logger.info(theSchema.getSchemaDefinition());

        byte[] dataFromEncoded = gsrDeserializer.getData(gsrEncodedBytes);
        Object thing = dataFormatDeserializer.deserialize(ByteBuffer.wrap(gsrEncodedBytes), theSchema.getSchemaDefinition());
        logger.info(thing.toString());

        logger.info("done...");
    }
}
