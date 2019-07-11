package com.spark.dom;

import net.sf.json.JSONObject;

public class CurrencyRate {
    private Float RMB;
    private Float USD;
    private Float JPY;
    private Float EUR;

    public CurrencyRate(String rmb, String usd, String jpy, String eur) {
        RMB = Float.valueOf(rmb);
        USD = Float.valueOf(usd);
        JPY = Float.valueOf(jpy);
        EUR = Float.valueOf(eur);
    }

    public CurrencyRate(JSONObject json) {
        RMB = Float.valueOf(json.getString("RMB"));
        USD = Float.valueOf(json.getString("USD"));
        JPY = Float.valueOf(json.getString("JPY"));
        EUR = Float.valueOf(json.getString("EUR"));
    }

    public Float getRate(String currency){
        switch (currency) {
            case "RMB": return RMB;
            case "CNY": return RMB;
            case "USD": return USD;
            case "JPY": return JPY;
            case "EUR": return EUR;
            default: return Float.valueOf(0);
        }
    }

    public String toString() {
        return "{ RMB: " + RMB + ", " +
                "USD: " + USD + ", " +
                "JPY: " + JPY + ", " +
                "EUR: " + EUR + " }";
    }
}
