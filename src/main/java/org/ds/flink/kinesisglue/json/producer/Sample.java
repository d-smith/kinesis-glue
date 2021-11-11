package org.ds.flink.kinesisglue.json.producer;

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

public class Sample {
    private static  Logger logger = LoggerFactory.getLogger(Sample.class);

    public static void main(String... args) {
        String jsonSchema = "{\n" + "        \"$schema\": \"http://json-schema.org/draft-04/schema#\",\n"
                + "        \"type\": \"object\",\n" + "        \"properties\": {\n" + "          \"employee\": {\n"
                + "            \"type\": \"object\",\n" + "            \"properties\": {\n"
                + "              \"name\": {\n" + "                \"type\": \"string\"\n" + "              },\n"
                + "              \"age\": {\n" + "                \"type\": \"integer\"\n" + "              },\n"
                + "              \"city\": {\n" + "                \"type\": \"string\"\n" + "              }\n"
                + "            },\n" + "            \"required\": [\n" + "              \"name\",\n"
                + "              \"age\",\n" + "              \"city\"\n" + "            ]\n" + "          }\n"
                + "        },\n" + "        \"required\": [\n" + "          \"employee\"\n" + "        ]\n"
                + "      }";
        String jsonPayload = "{\n" + "        \"employee\": {\n" + "          \"name\": \"John\",\n" + "          \"age\": 30,\n"
                + "          \"city\": \"New York\"\n" + "        }\n" + "      }";

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

        Schema gsrSchema =
                new Schema(dataFormatSerializer.getSchemaDefinition(record), dataFormat.name(), "MySchema");

        byte[] serializedBytes = dataFormatSerializer.serialize(record);

        byte[] gsrEncodedBytes = glueSchemaRegistrySerializer.encode("kpltest", gsrSchema, serializedBytes);

        GlueSchemaRegistryDeserializer gsrDeserializer =
                new GlueSchemaRegistryDeserializerImpl(awsCredentialsProvider, gsrConfig);
        GlueSchemaRegistryDataFormatDeserializer dataFormatDeserializer =
                new GlueSchemaRegistryDeserializerFactory().getInstance(dataFormat, gsrConfig);

        Schema theSchema = gsrDeserializer.getSchema(gsrEncodedBytes);

        logger.info(theSchema.getSchemaDefinition());

        logger.info("done...");
    }
}
