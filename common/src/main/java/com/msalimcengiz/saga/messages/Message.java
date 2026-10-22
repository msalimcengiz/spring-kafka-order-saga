package com.msalimcengiz.saga.messages;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.math.BigDecimal;
import java.util.UUID;

/**
 * Every command and event exchanged by the services. All of them are keyed by the
 * order id, so every message about one order lands on the same partition and is
 * consumed in order.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = Message.ChargePayment.class, name = "ChargePayment"),
    @JsonSubTypes.Type(value = Message.RefundPayment.class, name = "RefundPayment"),
    @JsonSubTypes.Type(value = Message.ReserveStock.class, name = "ReserveStock"),
    @JsonSubTypes.Type(value = Message.PaymentCharged.class, name = "PaymentCharged"),
    @JsonSubTypes.Type(value = Message.PaymentDeclined.class, name = "PaymentDeclined"),
    @JsonSubTypes.Type(value = Message.PaymentRefunded.class, name = "PaymentRefunded"),
    @JsonSubTypes.Type(value = Message.StockReserved.class, name = "StockReserved"),
    @JsonSubTypes.Type(value = Message.StockRejected.class, name = "StockRejected"),
    @JsonSubTypes.Type(value = Message.OrderCompleted.class, name = "OrderCompleted"),
    @JsonSubTypes.Type(value = Message.OrderCancelled.class, name = "OrderCancelled"),
})
public sealed interface Message {

    UUID orderId();

    /** The topic this message is published to. */
    String topic();

    // Commands sent by the order saga.

    record ChargePayment(UUID orderId, String customerId, BigDecimal amount) implements Message {
        public String topic() { return Topics.PAYMENT_COMMANDS; }
    }

    record RefundPayment(UUID orderId) implements Message {
        public String topic() { return Topics.PAYMENT_COMMANDS; }
    }

    record ReserveStock(UUID orderId, String productId, int quantity) implements Message {
        public String topic() { return Topics.INVENTORY_COMMANDS; }
    }

    // Replies from the participants.

    record PaymentCharged(UUID orderId, BigDecimal amount) implements Message {
        public String topic() { return Topics.PAYMENT_EVENTS; }
    }

    record PaymentDeclined(UUID orderId, String reason) implements Message {
        public String topic() { return Topics.PAYMENT_EVENTS; }
    }

    record PaymentRefunded(UUID orderId, BigDecimal amount) implements Message {
        public String topic() { return Topics.PAYMENT_EVENTS; }
    }

    record StockReserved(UUID orderId) implements Message {
        public String topic() { return Topics.INVENTORY_EVENTS; }
    }

    record StockRejected(UUID orderId, String reason) implements Message {
        public String topic() { return Topics.INVENTORY_EVENTS; }
    }

    // Outcome events for anyone else interested (notifications, analytics, ...).

    record OrderCompleted(UUID orderId) implements Message {
        public String topic() { return Topics.ORDER_EVENTS; }
    }

    record OrderCancelled(UUID orderId, String reason) implements Message {
        public String topic() { return Topics.ORDER_EVENTS; }
    }
}
