package com.msalimcengiz.saga.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.msalimcengiz.saga.messages.Message;
import com.msalimcengiz.saga.messages.Message.ChargePayment;
import com.msalimcengiz.saga.messages.Message.OrderCancelled;
import com.msalimcengiz.saga.messages.Message.OrderCompleted;
import com.msalimcengiz.saga.messages.Message.PaymentCharged;
import com.msalimcengiz.saga.messages.Message.PaymentDeclined;
import com.msalimcengiz.saga.messages.Message.PaymentRefunded;
import com.msalimcengiz.saga.messages.Message.RefundPayment;
import com.msalimcengiz.saga.messages.Message.ReserveStock;
import com.msalimcengiz.saga.messages.Message.StockRejected;
import com.msalimcengiz.saga.messages.Message.StockReserved;
import com.msalimcengiz.saga.order.OrderSaga.Status;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class OrderSagaTest {

    private static final UUID ID = UUID.randomUUID();
    private static final BigDecimal AMOUNT = new BigDecimal("120.00");

    private static Order order(Status s) {
        return new Order(ID, "c001", "p001", 2, AMOUNT, s, s == Status.REFUND_PENDING ? "out_of_stock: x" : null, Instant.now(), Instant.now());
    }

    @Test
    void happyPath() {
        var charged = OrderSaga.on(order(Status.PAYMENT_PENDING), new PaymentCharged(ID, AMOUNT)).orElseThrow();
        assertThat(charged.next()).isEqualTo(Status.STOCK_PENDING);
        assertThat(charged.send()).containsExactly(new ReserveStock(ID, "p001", 2));

        var reserved = OrderSaga.on(order(Status.STOCK_PENDING), new StockReserved(ID)).orElseThrow();
        assertThat(reserved.next()).isEqualTo(Status.COMPLETED);
        assertThat(reserved.send()).containsExactly(new OrderCompleted(ID));
    }

    @Test
    void declinedPaymentCancelsWithoutCompensation() {
        var t = OrderSaga.on(order(Status.PAYMENT_PENDING), new PaymentDeclined(ID, "insufficient_funds")).orElseThrow();
        assertThat(t.next()).isEqualTo(Status.CANCELLED);
        assertThat(t.cancelReason()).isEqualTo("payment_declined: insufficient_funds");
        assertThat(t.send()).containsExactly(new OrderCancelled(ID, "payment_declined"));
    }

    @Test
    void outOfStockRefundsBeforeCancelling() {
        var rejected = OrderSaga.on(order(Status.STOCK_PENDING), new StockRejected(ID, "insufficient_stock")).orElseThrow();
        assertThat(rejected.next()).isEqualTo(Status.REFUND_PENDING);
        assertThat(rejected.send()).containsExactly(new RefundPayment(ID));

        var refunded = OrderSaga.on(order(Status.REFUND_PENDING), new PaymentRefunded(ID, AMOUNT)).orElseThrow();
        assertThat(refunded.next()).isEqualTo(Status.CANCELLED);
        assertThat(refunded.cancelReason()).isEqualTo("out_of_stock: x");
        assertThat(refunded.send()).containsExactly(new OrderCancelled(ID, "out_of_stock"));
    }

    @Test
    void repliesThatDoNotFitTheStateAreIgnored() {
        // Duplicate PaymentCharged after the saga moved on (e.g. reply to a re-sent command).
        assertThat(OrderSaga.on(order(Status.STOCK_PENDING), new PaymentCharged(ID, AMOUNT))).isEmpty();
        // Late StockReserved for an order that was already completed.
        assertThat(OrderSaga.on(order(Status.COMPLETED), new StockReserved(ID))).isEmpty();
        // Nothing moves a final order.
        for (Message m : new Message[] {new PaymentDeclined(ID, "x"), new StockRejected(ID, "x"), new PaymentRefunded(ID, AMOUNT)}) {
            assertThat(OrderSaga.on(order(Status.CANCELLED), m)).isEmpty();
        }
    }

    @ParameterizedTest
    @EnumSource(Status.class)
    void retryCommandMatchesTheState(Status s) {
        var cmd = OrderSaga.retryCommand(order(s));
        switch (s) {
            case PAYMENT_PENDING -> assertThat(cmd).contains(new ChargePayment(ID, "c001", AMOUNT));
            case STOCK_PENDING -> assertThat(cmd).contains(new ReserveStock(ID, "p001", 2));
            case REFUND_PENDING -> assertThat(cmd).contains(new RefundPayment(ID));
            case COMPLETED, CANCELLED -> assertThat(cmd).isEmpty();
        }
    }
}
