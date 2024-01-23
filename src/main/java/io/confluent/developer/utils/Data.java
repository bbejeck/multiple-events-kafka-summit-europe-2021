package io.confluent.developer.utils;

import io.confluent.developer.avro.PageView;
import io.confluent.developer.avro.Purchase;

public class Data {

    public static PageView avroPageView() {
        return PageView.newBuilder()
                .setCustomerId("vandelay1234")
                .setIsSpecial(true)
                .setUrl("https://acme.commerce/sale")
                .build();
        
    }

    public static Purchase avroPurchase() {
        return Purchase.newBuilder()
                .setCustomerId("vandelay1234")
                .setAmount(437.83)
                .setItem("flux-capacitor")
                .build();
    }

    public static io.confluent.developer.json.Purchase jsonSchemaPurchase() {
        return new io.confluent.developer.json.Purchase()
                .withAmount(437.83)
                .withCustomerId("vandelay1234")
                .withItem("flux-capacitor");
    }

    public static io.confluent.developer.json.PageView jsonSchemaPageView() {
        return new io.confluent.developer.json.PageView()
                .withCustomerId("vandelay1234")
                .withIsSpecial(true)
                .withUrl("https://acme.commerce/sale");
    }

    public static PageView protoPageView() {
        return PageView.newBuilder()
                .setCustomerId("vandelay1234")
                .setIsSpecial(true)
                .setUrl("https://acme.commerce/sale")
                .build();
    }

    public static Purchase protoPurchase() {
        return Purchase.newBuilder()
                .setCustomerId("vandelay1234")
                .setAmount(437.83)
                .setItem("flux-capacitor")
                .build();
    }
}
