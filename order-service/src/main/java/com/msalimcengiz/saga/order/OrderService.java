package com.msalimcengiz.saga.order;

import com.msalimcengiz.saga.messages.Message;
import com.msalimcengiz.saga.messages.Message.ChargePayment;
import com.msalimcengiz.saga.order.OrderSaga.Status;
import com.msalimcengiz.saga.outbox.OutboxWriter;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    public record Placed(Order order, boolean created) {}

    private final OrderRepository orders;
    private final OutboxWriter outbox;
    private final MeterRegistry meters;
    private final Duration stuckAfter;

    public OrderService(
        OrderRepository orders,
        OutboxWriter outbox,
        MeterRegistry meters,
        @Value("${saga.stuck-after:PT2M}") Duration stuckAfter
    ) {
        this.orders = orders;
        this.outbox = outbox;
        this.meters = meters;
        this.stuckAfter = stuckAfter;
    }

    /**
     * Stores the order and its first command in one transaction. A repeated request with
     * the same Idempotency-Key returns the original order and starts nothing new.
     */
    @Transactional
    public Placed place(String idempotencyKey, String customerId, String productId, int quantity, BigDecimal amount) {
        var order = new Order(UUID.randomUUID(), customerId, productId, quantity, amount,
            Status.PAYMENT_PENDING, null, Instant.now(), Instant.now());
        var inserted = orders.insertIfAbsent(idempotencyKey, order);
        if (inserted.isEmpty()) {
            return new Placed(orders.findByIdempotencyKey(idempotencyKey).orElseThrow(), false);
        }
        outbox.append(new ChargePayment(order.id(), customerId, amount));
        meters.counter("saga.orders.placed").increment();
        return new Placed(order, true);
    }

    /** Applies a participant's reply. Called inside the listener's transaction. */
    @Transactional
    public void apply(Message reply) {
        var order = orders.lock(reply.orderId()).orElse(null);
        if (order == null) {
            // Every reply answers a command this service sent for an order it stored, so
            // this means data was lost or the message is foreign. Retrying cannot help.
            throw new IllegalStateException("reply for unknown order " + reply.orderId());
        }
        OrderSaga.on(order, reply).ifPresentOrElse(t -> {
            orders.update(order.id(), t.next(), t.cancelReason());
            t.send().forEach(outbox::append);
            meters.counter("saga.transitions", "to", t.next().name()).increment();
        }, () -> {
            meters.counter("saga.replies.ignored", "type", reply.getClass().getSimpleName()).increment();
            log.debug("ignoring {} for order {} in state {}", reply.getClass().getSimpleName(), order.id(), order.status());
        });
    }

    /**
     * Re-sends the current command for orders stuck in a pending state (a reply that went
     * to a DLT and was fixed, a participant that was down for long). Safe because every
     * participant handles each command idempotently per order.
     */
    @Scheduled(fixedDelayString = "${saga.sweep-interval-ms:30000}")
    @Transactional
    public void resendStuck() {
        for (Order o : orders.lockStuck(stuckAfter, 100)) {
            OrderSaga.retryCommand(o).ifPresent(cmd -> {
                outbox.append(cmd);
                orders.touch(o.id());
                meters.counter("saga.commands.resent", "status", o.status().name()).increment();
                log.warn("order {} stuck in {} since {}, re-sent {}", o.id(), o.status(), o.updatedAt(),
                    cmd.getClass().getSimpleName());
            });
        }
    }
}
