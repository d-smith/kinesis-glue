package org.ds.flink.kinesisglue.avro.application;

public class Quote {
    public String symbol;
    public Double price;

    public Quote() {
    }

    public Quote(String symbol, Double price) {
        this.symbol = symbol;
        this.price = price;
    }

    @Override
    public String toString() {
        return "Quote{" +
                "symbol='" + symbol + '\'' +
                ", price=" + price +
                '}';
    }
}
