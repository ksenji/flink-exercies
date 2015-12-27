package org.myorg.quickstart;

import java.io.Serializable;

public class StockPrice implements Serializable {

    private String symbol;
    private Double price;

    public StockPrice() {
    }

    public StockPrice(String symbol, Double price) {
        this.symbol = symbol;
        this.price = price;
    }

    public String getSymbol() {
        return symbol;
    }

    public Double getPrice() {
        return price;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String toString() {
        return String.format("StockPrice {symbol='%s', price=%f}", symbol, price);
    }
}
