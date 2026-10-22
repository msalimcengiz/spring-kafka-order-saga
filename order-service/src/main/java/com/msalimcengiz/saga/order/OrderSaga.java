package com.msalimcengiz.saga.order;

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
import java.util.List;
import java.util.Optional;

/**
 * The saga as a pure state machine: given the order and a reply, returns the next
 * state and the messages to send. No I/O, so every path is unit-tested.
 *
 * <pre>
 *  PAYMENT_PENDING --PaymentCharged--> STOCK_PENDING --StockReserved--> COMPLETED
 *        |                                   |
 *  PaymentDeclined                      StockRejected
 *        v                                   v
 *    CANCELLED  <------PaymentRefunded--- REFUND_PENDING
 * </pre>
 *
 * A reply that does not fit the current state (a duplicate, or a reply to a command
 * the sweeper re-sent) returns empty and is ignored.
 */
public final class OrderSaga {

    public enum Status { PAYMENT_PENDING, STOCK_PENDING, REFUND_PENDING, COMPLETED, CANCELLED }

    public record Transition(Status next, String cancelReason, List<Message> send) {}

    public static Optional<Transition> on(Order order, Message reply) {
        return Optional.ofNullable(switch (reply) {
            case PaymentCharged c when order.status() == Status.PAYMENT_PENDING ->
                new Transition(Status.STOCK_PENDING, null,
                    List.of(new ReserveStock(order.id(), order.productId(), order.quantity())));
            case PaymentDeclined d when order.status() == Status.PAYMENT_PENDING ->
                new Transition(Status.CANCELLED, "payment_declined: " + d.reason(),
                    List.of(new OrderCancelled(order.id(), "payment_declined")));
            case StockReserved r when order.status() == Status.STOCK_PENDING ->
                new Transition(Status.COMPLETED, null, List.of(new OrderCompleted(order.id())));
            case StockRejected r when order.status() == Status.STOCK_PENDING ->
                // Compensation: the customer was charged, so refund before cancelling.
                new Transition(Status.REFUND_PENDING, "out_of_stock: " + r.reason(),
                    List.of(new RefundPayment(order.id())));
            case PaymentRefunded r when order.status() == Status.REFUND_PENDING ->
                new Transition(Status.CANCELLED, order.cancelReason(),
                    List.of(new OrderCancelled(order.id(), "out_of_stock")));
            default -> null;
        });
    }

    /** The command to re-send when an order has waited too long in its current state. */
    public static Optional<Message> retryCommand(Order order) {
        return Optional.ofNullable(switch (order.status()) {
            case PAYMENT_PENDING -> new ChargePayment(order.id(), order.customerId(), order.amount());
            case STOCK_PENDING -> new ReserveStock(order.id(), order.productId(), order.quantity());
            case REFUND_PENDING -> new RefundPayment(order.id());
            case COMPLETED, CANCELLED -> null;
        });
    }

    private OrderSaga() {}
}
