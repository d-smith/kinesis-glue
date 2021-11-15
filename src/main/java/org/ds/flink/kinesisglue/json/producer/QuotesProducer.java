package org.ds.flink.kinesisglue.json.producer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QuotesProducer {
    static Logger LOG = LoggerFactory.getLogger(QuotesProducer.class);

    public static void main(String... args) throws Exception {

        String[] symbols = {
                "DWAC",
                "SNDL",
                "CEI",
                "FAMI",
                "KMDN"
        };

        final String QUOTE_SCHEMA = "{" +
                "\"$schema\": \"http://json-schema.org/draft-04/schema#\"," +
                "\"type\":\"object\"," +
                "\"properties\": {" +
                "\"symbol\":{\"type\":\"string\"}," +
                "\"price\":{\"type\":\"number\"}" +
                "}," +
                "\"required\":[\"symbol\",\"price\"]}";

      StreamWriter streamWriter = new StreamWriter(QUOTE_SCHEMA);

        for (; ; ) {

            int idx = (int) (Math.random() * symbols.length);
            String symbol = symbols[idx];
            double price = Math.random() * 600;

            LOG.info("quoting {} {}", symbol, price);
            String payload = String.format("{ \"symbol\":\"%s\",\"price\":%f }",symbol, price);
            streamWriter.writeToStream("kpltest", symbol, payload);
            Thread.sleep(15);
        }
    }
}

