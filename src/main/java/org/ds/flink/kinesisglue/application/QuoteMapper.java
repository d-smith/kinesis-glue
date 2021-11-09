package org.ds.flink.kinesisglue.application;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.flink.api.common.functions.MapFunction;

public class QuoteMapper implements MapFunction<GenericRecord, Quote> {

    @Override
    public Quote map(GenericRecord gr) throws Exception {
        String symbol = (((Utf8)gr.get("symbol")).toString());
        return new  Quote(symbol, (double)gr.get("price"));
    }
}
