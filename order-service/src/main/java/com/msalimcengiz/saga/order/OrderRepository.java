package com.msalimcengiz.saga.order;

import com.msalimcengiz.saga.order.OrderSaga.Status;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class OrderRepository {

    private static final String COLUMNS =
        "id, customer_id, product_id, quantity, amount, status, cancel_reason, created_at, updated_at";

    private final JdbcClient jdbc;

    public OrderRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Inserts unless the idempotency key exists; returns the id of the stored order and whether it is new. */
    public Optional<UUID> insertIfAbsent(String idempotencyKey, Order o) {
        return jdbc.sql("""
                INSERT INTO orders (id, idempotency_key, customer_id, product_id, quantity, amount, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING id""")
            .params(o.id(), idempotencyKey, o.customerId(), o.productId(), o.quantity(), o.amount(), o.status().name())
            .query(UUID.class)
            .optional();
    }

    public Optional<Order> findByIdempotencyKey(String key) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM orders WHERE idempotency_key = ?").param(key)
            .query(OrderRepository::map).optional();
    }

    public Optional<Order> find(UUID id) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM orders WHERE id = ?").param(id)
            .query(OrderRepository::map).optional();
    }

    /** Locks the row for the rest of the transaction, so replies for one order are applied one at a time. */
    public Optional<Order> lock(UUID id) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM orders WHERE id = ? FOR UPDATE").param(id)
            .query(OrderRepository::map).optional();
    }

    public void update(UUID id, Status status, String cancelReason) {
        jdbc.sql("UPDATE orders SET status = ?, cancel_reason = ?, updated_at = now() WHERE id = ?")
            .params(status.name(), cancelReason, id)
            .update();
    }

    /** Orders that have waited longer than {@code timeout} in a non-final state. Rows are locked; others skip them. */
    public List<Order> lockStuck(Duration timeout, int limit) {
        return jdbc.sql("SELECT " + COLUMNS + """
                 FROM orders
                WHERE status IN ('PAYMENT_PENDING', 'STOCK_PENDING', 'REFUND_PENDING')
                  AND updated_at < now() - make_interval(secs => ?)
                ORDER BY updated_at LIMIT ? FOR UPDATE SKIP LOCKED""")
            .params(timeout.toSeconds(), limit)
            .query(OrderRepository::map)
            .list();
    }

    public void touch(UUID id) {
        jdbc.sql("UPDATE orders SET updated_at = now() WHERE id = ?").param(id).update();
    }

    private static Order map(ResultSet rs, int n) throws SQLException {
        return new Order(
            rs.getObject("id", UUID.class),
            rs.getString("customer_id"),
            rs.getString("product_id"),
            rs.getInt("quantity"),
            rs.getBigDecimal("amount"),
            Status.valueOf(rs.getString("status")),
            rs.getString("cancel_reason"),
            rs.getTimestamp("created_at").toInstant(),
            rs.getTimestamp("updated_at").toInstant());
    }
}
