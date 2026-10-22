package com.msalimcengiz.saga.inventory;

import com.msalimcengiz.saga.fault.FaultInjector;
import com.msalimcengiz.saga.messages.Message;
import com.msalimcengiz.saga.messages.Message.ReserveStock;
import com.msalimcengiz.saga.messages.Message.StockRejected;
import com.msalimcengiz.saga.messages.Message.StockReserved;
import com.msalimcengiz.saga.outbox.OutboxWriter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** Reserves stock, idempotent per order. Stock can never go negative: the decrement is conditional. */
@Component
public class InventoryHandler {

    private final JdbcClient jdbc;
    private final OutboxWriter outbox;
    private final FaultInjector faults;
    private final MeterRegistry meters;

    public InventoryHandler(JdbcClient jdbc, OutboxWriter outbox, FaultInjector faults, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.outbox = outbox;
        this.faults = faults;
        this.meters = meters;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void handle(Message command) {
        if (!(command instanceof ReserveStock r)) {
            throw new IllegalArgumentException("inventory-service cannot handle " + command.getClass().getSimpleName());
        }
        int claimed = jdbc.sql("""
                INSERT INTO reservations (order_id, product_id, quantity, status)
                VALUES (?, ?, ?, 'REJECTED') ON CONFLICT (order_id) DO NOTHING""")
            .params(r.orderId(), r.productId(), r.quantity())
            .update();
        if (claimed == 0) {
            meters.counter("inventory.commands.repeated").increment();
            String status = jdbc.sql("SELECT status FROM reservations WHERE order_id = ?")
                .param(r.orderId()).query(String.class).single();
            outbox.append("RESERVED".equals(status) ? new StockReserved(r.orderId()) : new StockRejected(r.orderId(), "insufficient_stock"));
            return;
        }

        faults.maybeFail("reserve");

        int reserved = jdbc.sql("UPDATE products SET available = available - ? WHERE id = ? AND available >= ?")
            .params(r.quantity(), r.productId(), r.quantity())
            .update();
        if (reserved == 1) {
            jdbc.sql("UPDATE reservations SET status = 'RESERVED' WHERE order_id = ?").param(r.orderId()).update();
            outbox.append(new StockReserved(r.orderId()));
            meters.counter("inventory.reserved").increment();
        } else {
            outbox.append(new StockRejected(r.orderId(), "insufficient_stock"));
            meters.counter("inventory.rejected").increment();
        }
    }
}
