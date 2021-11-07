package org.ds.flink.kinesisglue.producer;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;


public class StreamWriter {

    private static final Random RANDOM = new Random();
    private final static Logger logger = LoggerFactory.getLogger(StreamWriter.class);

    private KinesisProducer kinesisProducer;

    final ExecutorService callbackThreadPool = Executors.newCachedThreadPool();
    final long outstandingLimit = KPLConfiguration.getBackPressureBufferThreshold();
    final long maxBackpressureTries = 5000;
    private long errorCount;
    private Schema quoteSchema;
    private org.apache.avro.Schema avroSchema;



    public StreamWriter(String schema) {

        //Create schema registry and avro schemas
        this.quoteSchema =
                new Schema(schema, DataFormat.AVRO.toString(), "quoteSchema");
        this.avroSchema = new org.apache.avro.Schema.Parser().parse(schema);

        //For convenience enable auto-registration
        GlueSchemaRegistryConfiguration schemaRegistryConfig =
                new GlueSchemaRegistryConfiguration(System.getenv("AWS_REGION"));
        schemaRegistryConfig.setSchemaAutoRegistrationEnabled(true);
        schemaRegistryConfig.setRegistryName("registry-sample");
        schemaRegistryConfig.setDescription("registry to store schema for sample appllication");
        schemaRegistryConfig.setCacheSize(100);
        schemaRegistryConfig.setTimeToLiveMillis(24*60*60*1000);

        logger.info("initializing KPL");
        KinesisProducerConfiguration config = new KinesisProducerConfiguration()
                .setGlueSchemaRegistryConfiguration(schemaRegistryConfig)
                .setFailIfThrottled(KPLConfiguration.getFailIfThrottled())
                .setRecordMaxBufferedTime(KPLConfiguration.getRecordMaxBufferedTime())
                .setMaxConnections(10)
                .setRegion(System.getenv("AWS_REGION"))
                .setRateLimit(KPLConfiguration.getRateLimit())
                .setRecordTtl(KPLConfiguration.getRecordTtl())
                .setRequestTimeout(60000);

        kinesisProducer = new KinesisProducer(config);
    }

    public void writeToStream(String streamName, String partitionKey, String symbol, double price) throws Exception {

        //Use the following for overloading the partition key if you are testing with static.
        //Make sure to update the addUserRecord call below to use the overridden value
        //JMeter payloads against multiple shards
        //String overridePartitionKey = UUID.randomUUID().toString();

        GenericRecord quote = new GenericData.Record(this.avroSchema);
        quote.put("symbol",symbol);
        quote.put("price", price);
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().directBinaryEncoder(outBytes,null);
        new GenericDatumWriter<>(this.avroSchema).write(quote, encoder);
        encoder.flush();

        int attempts = 0;
        while(attempts < maxBackpressureTries) {

            if (kinesisProducer.getOutstandingRecordsCount() < outstandingLimit) {
                //if(attempts > 0) {
                //    logger.info("add user record to producer after {} attempts", attempts);
                //}

                //You can watch this and see the crash coming....
                //logger.info("-------> OLDEST: {}",kinesisProducer.getOldestRecordTimeInMillis());


                ByteBuffer buffer = ByteBuffer.wrap(outBytes.toByteArray());

                // doesn't block
                ListenableFuture<UserRecordResult> f = //kinesisProducer.addUserRecord(streamName, partitionKey, buffer);
                        kinesisProducer.addUserRecord(streamName,symbol,null,buffer, this.quoteSchema);
                Futures.addCallback(f, new FutureCallback<UserRecordResult>() {
                    @Override
                    public void onSuccess(UserRecordResult result) {
                        long totalTime = result.getAttempts().stream()
                                .mapToLong(a -> a.getDelay() + a.getDuration())
                                .sum();
                        // Only log with a small probability, otherwise it'll be very
                        // spammy
                        if (RANDOM.nextDouble() < 1e-5) {
                            logger.info(String.format(
                                    "Succesfully put record, partitionKey=%s, "
                                            + "payload=%s, sequenceNumber=%s, "
                                            + "shardId=%s, took %d attempts, "
                                            + "totalling %s ms",
                                    partitionKey, outBytes, result.getSequenceNumber(),
                                    result.getShardId(), result.getAttempts().size(),
                                    totalTime));
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        if (t instanceof UserRecordFailedException) {
                            UserRecordFailedException e =
                                    (UserRecordFailedException) t;
                            UserRecordResult result = e.getResult();

                            String errorList =
                                    StringUtils.join(result.getAttempts().stream()
                                            .map(a -> String.format(
                                                    "Delay after prev attempt: %d ms, "
                                                            + "Duration: %d ms, Code: %s, "
                                                            + "Message: %s",
                                                    a.getDelay(), a.getDuration(),
                                                    a.getErrorCode(),
                                                    a.getErrorMessage()))
                                            .collect(Collectors.toList()), "\n");

                            logger.error(String.format(
                                    "Record failed to put, partitionKey=%s, "
                                            + "payload=%s, attempts:\n%s",
                                    partitionKey, outBytes, errorList));
                        }
                    }

                    ;
                }, callbackThreadPool);

                break;
            } else {

                attempts++;
                try {
                    Thread.sleep(1);
                } catch (Throwable t) {
                    logger.error("interrupted exception thrown while attempting to apply backpressure");
                }
            }
        }

        if(attempts == maxBackpressureTries ) {
            logger.error("Gave up after {} attempts", maxBackpressureTries);
        }
    }
}
