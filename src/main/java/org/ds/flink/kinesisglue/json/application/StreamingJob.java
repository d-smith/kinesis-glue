package org.ds.flink.kinesisglue.json.application;

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatDeserializer;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerFactory;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Properties;


public class StreamingJob {
    private static final String region = "us-east-1";
    private static final String inputStreamName = "kpltest";


    public static class GSRConfigHolder {
        private static final GSRConfigHolder instance = new GSRConfigHolder();

        public static GSRConfigHolder Instance() {
            return instance;
        }

        private GlueSchemaRegistryConfiguration gsrConfig;

        private GSRConfigHolder() {
            gsrConfig = new GlueSchemaRegistryConfiguration(System.getenv("AWS_REGION"));
            gsrConfig.setSchemaAutoRegistrationEnabled(true);
            gsrConfig.setRegistryName("registry-sample");
            gsrConfig.setDescription("registry to store schema for sample appllication");
            gsrConfig.setCacheSize(100);
            gsrConfig.setTimeToLiveMillis(24 * 60 * 60 * 1000);
        }

        public GlueSchemaRegistryConfiguration getGsrConfig() {
            return gsrConfig;
        }
    }


    public static class JsonCracker implements DeserializationSchema<Quote>, Serializable {

        private final String QUOTE_SCHEMA = "{" +
                "\"$schema\": \"http://json-schema.org/draft-04/schema#\"," +
                "\"className\":\"org.ds.flink.kinesisglue.json.application.Quote\"," +
                "\"type\":\"object\"," +
                "\"properties\": {" +
                "\"symbol\":{\"type\":\"string\"}," +
                "\"price\":{\"type\":\"number\"}" +
                "}," +
                "\"required\":[\"symbol\",\"price\"]}";

        @Override
        public Quote deserialize(byte[] bytes) throws IOException {

            GlueSchemaRegistryConfiguration gsrConfig = GSRConfigHolder.Instance().getGsrConfig();
            GlueSchemaRegistryDataFormatDeserializer dataFormatDeserializer =
                    new GlueSchemaRegistryDeserializerFactory().getInstance(DataFormat.JSON, gsrConfig);
            Object deserialized = dataFormatDeserializer.deserialize(ByteBuffer.wrap(bytes), QUOTE_SCHEMA);
            return (Quote) deserialized;
        }

        @Override
        public boolean isEndOfStream(Quote q) {
            return false;
        }

        @Override
        public TypeInformation<Quote> getProducedType() {
            return TypeInformation.of(Quote.class);
        }
    }

    private static DataStream<Quote> createNonSchemaSourceFromStaticConfig(StreamExecutionEnvironment env) {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");


        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new JsonCracker(), inputProperties));
    }

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //DataStream<GenericRecord> input = createSourceFromStaticConfig(env);
        DataStream<Quote> input = createNonSchemaSourceFromStaticConfig(env);
        input
                /*.map(new QuoteMapper())*/
                .keyBy(quote -> quote.symbol)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .reduce(new ReduceFunction<Quote>() {
                    @Override
                    public Quote reduce(Quote quote, Quote t1) throws Exception {
                        return t1; //Conflate all quotes in a window to the last quote
                    }
                })
                .print();

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}
