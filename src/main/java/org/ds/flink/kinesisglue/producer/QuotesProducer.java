package org.ds.flink.kinesisglue.producer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;


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

        final String QUOTE_SCHEMA = "{\"namespace\":\"Quote.avro\",\n"
                + "\"type\":\"record\",\n"
                + "\"name\":\"Quote\",\n"
                + "\"fields\":[\n"
                + "{\"name\":\"symbol\", \"type\":\"string\"},\n"
                + "{\"name\":\"price\", \"type\":\"double\"}\n"
                + "]\n"
                + "}";

      StreamWriter streamWriter = new StreamWriter(QUOTE_SCHEMA);

        for (; ; ) {

            int idx = (int) (Math.random() * symbols.length);
            String symbol = symbols[idx];
            double price = Math.random() * 600;

            LOG.info("quoting {} {}", symbol, price);
            streamWriter.writeToStream("kpltest", symbol, symbol, price);
            Thread.sleep(15);
        }
    }
}

