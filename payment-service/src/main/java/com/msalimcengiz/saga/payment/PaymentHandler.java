package com.msalimcengiz.saga.payment;

import com.msalimcengiz.saga.fault.FaultInjector;
import com.msalimcengiz.saga.messages.Message;
import com.msalimcengiz.saga.messages.Message.ChargePayment;
import com.msalimcengiz.saga.messages.Message.PaymentCharged;
import com.msalimcengiz.saga.messages.Message.PaymentDeclined;
import com.msalimcengiz.saga.messages.Message.PaymentRefunded;
import com.msalimcengiz.saga.messages.Message.RefundPayment;
import com.msalimcengiz.saga.outbox.OutboxWriter;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Charges and refunds, idempotent per order. A repeated command (redelivery, or the order
 * saga re-sending after a timeout) never moves money twice; it re-sends the original answer.
 */
@Component
public class PaymentHandler {

    private record Payment(String status, BigDecimal amount, String reason) {}

    private final JdbcClient jdbc;
    private final OutboxWriter outbox;
    private final FaultInjector faults;
    private final MeterRegistry meters;

    public PaymentHandler(JdbcClient jdbc, OutboxWriter outbox, FaultInjector faults, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.outbox = outbox;
        this.faults = faults;
        this.meters = meters;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void handle(Message command) {
        switch (command) {
            case ChargePayment c -> charge(c);
            case RefundPayment r -> refund(r);
            default -> throw new IllegalArgumentException("payment-service cannot handle " + command.getClass().getSimpleName());
        }
    }

    private void charge(ChargePayment c) {
        // Claim the order first. ON CONFLICT makes two concurrent deliveries of the same
        // command race on the primary key instead of both debiting the account.
        int claimed = jdbc.sql("""
                INSERT INTO payments (order_id, customer_id, amount, status)
                VALUES (?, ?, ?, 'DECLINED') ON CONFLICT (order_id) DO NOTHING""")
            .params(c.orderId(), c.customerId(), c.amount())
            .update();
        if (claimed == 0) {
            meters.counter("payment.commands.repeated").increment();
            replyFor(c.orderId(), find(c.orderId()));
            return;
        }

        faults.maybeFail("charge");  // after the claim, so a failure must roll it back

        int debited = jdbc.sql("UPDATE accounts SET balance = balance - ? WHERE customer_id = ? AND balance >= ?")
            .params(c.amount(), c.customerId(), c.amount())
            .update();
        if (debited == 1) {
            jdbc.sql("UPDATE payments SET status = 'CHARGED' WHERE order_id = ?").param(c.orderId()).update();
            outbox.append(new PaymentCharged(c.orderId(), c.amount()));
            meters.counter("payment.charged").increment();
            return;
        }
        boolean known = jdbc.sql("SELECT EXISTS (SELECT 1 FROM accounts WHERE customer_id = ?)")
            .param(c.customerId()).query(Boolean.class).single();
        String reason = known ? "insufficient_funds" : "unknown_customer";
        jdbc.sql("UPDATE payments SET reason = ? WHERE order_id = ?").params(reason, c.orderId()).update();
        outbox.append(new PaymentDeclined(c.orderId(), reason));
        meters.counter("payment.declined", "reason", reason).increment();
    }

    private void refund(RefundPayment r) {
        var p = jdbc.sql("SELECT status, amount, reason FROM payments WHERE order_id = ? FOR UPDATE")
            .param(r.orderId())
            .query((rs, n) -> new Payment(rs.getString(1), rs.getBigDecimal(2), rs.getString(3)))
            .optional()
            .orElse(null);
        if (p == null || !"CHARGED".equals(p.status())) {
            // Already refunded, declined, or never charged: nothing to move, confirm anyway.
            outbox.append(new PaymentRefunded(r.orderId(), p != null && "REFUNDED".equals(p.status()) ? p.amount() : BigDecimal.ZERO));
            return;
        }
        faults.maybeFail("refund");
        jdbc.sql("""
                UPDATE accounts SET balance = balance + ?
                WHERE customer_id = (SELECT customer_id FROM payments WHERE order_id = ?)""")
            .params(p.amount(), r.orderId())
            .update();
        jdbc.sql("UPDATE payments SET status = 'REFUNDED', updated_at = now() WHERE order_id = ?").param(r.orderId()).update();
        outbox.append(new PaymentRefunded(r.orderId(), p.amount()));
        meters.counter("payment.refunded").increment();
    }

    private Payment find(java.util.UUID orderId) {
        return jdbc.sql("SELECT status, amount, reason FROM payments WHERE order_id = ?")
            .param(orderId)
            .query((rs, n) -> new Payment(rs.getString(1), rs.getBigDecimal(2), rs.getString(3)))
            .single();
    }

    private void replyFor(java.util.UUID orderId, Payment p) {
        outbox.append(switch (p.status()) {
            case "CHARGED", "REFUNDED" -> new PaymentCharged(orderId, p.amount());
            default -> new PaymentDeclined(orderId, p.reason());
        });
    }
}
