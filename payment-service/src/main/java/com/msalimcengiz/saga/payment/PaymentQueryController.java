package com.msalimcengiz.saga.payment;

import java.math.BigDecimal;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/** Read-only views, so balances and payments can be checked from outside (demo, support). */
@RestController
public class PaymentQueryController {

    public record Account(String customerId, BigDecimal balance) {}

    public record Payment(UUID orderId, String customerId, BigDecimal amount, String status, String reason) {}

    private final JdbcClient jdbc;

    public PaymentQueryController(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/accounts/{customerId}")
    public ResponseEntity<Account> account(@PathVariable String customerId) {
        return ResponseEntity.of(jdbc.sql("SELECT customer_id, balance FROM accounts WHERE customer_id = ?")
            .param(customerId)
            .query((rs, n) -> new Account(rs.getString(1), rs.getBigDecimal(2)))
            .optional());
    }

    @GetMapping("/payments/{orderId}")
    public ResponseEntity<Payment> payment(@PathVariable UUID orderId) {
        return ResponseEntity.of(jdbc.sql("SELECT order_id, customer_id, amount, status, reason FROM payments WHERE order_id = ?")
            .param(orderId)
            .query((rs, n) -> new Payment(rs.getObject(1, UUID.class), rs.getString(2), rs.getBigDecimal(3), rs.getString(4), rs.getString(5)))
            .optional());
    }
}
