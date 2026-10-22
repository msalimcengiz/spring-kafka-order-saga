package com.msalimcengiz.saga.inventory;

import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/** Read-only stock view, so reservations can be checked from outside (demo, support). */
@RestController
public class InventoryQueryController {

    public record Product(String id, int available) {}

    private final JdbcClient jdbc;

    public InventoryQueryController(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/products/{id}")
    public ResponseEntity<Product> product(@PathVariable String id) {
        return ResponseEntity.of(jdbc.sql("SELECT id, available FROM products WHERE id = ?")
            .param(id)
            .query((rs, n) -> new Product(rs.getString(1), rs.getInt(2)))
            .optional());
    }
}
