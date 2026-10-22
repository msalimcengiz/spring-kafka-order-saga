package com.msalimcengiz.saga.messages;

import java.util.List;

public final class Topics {
    public static final String PAYMENT_COMMANDS = "payments.commands";
    public static final String PAYMENT_EVENTS = "payments.events";
    public static final String INVENTORY_COMMANDS = "inventory.commands";
    public static final String INVENTORY_EVENTS = "inventory.events";
    public static final String ORDER_EVENTS = "orders.events";

    public static final List<String> ALL =
        List.of(PAYMENT_COMMANDS, PAYMENT_EVENTS, INVENTORY_COMMANDS, INVENTORY_EVENTS, ORDER_EVENTS);

    /** Records that failed processing end up here, on the same partition number. */
    public static String deadLetter(String topic) {
        return topic + ".DLT";
    }

    private Topics() {}
}
