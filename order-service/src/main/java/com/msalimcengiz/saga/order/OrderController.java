package com.msalimcengiz.saga.order;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    public record PlaceOrder(
        @NotBlank @Size(max = 64) String customerId,
        @NotBlank @Size(max = 64) String productId,
        @Min(1) @Max(100) int quantity,
        @NotNull @DecimalMin("0.01") @Digits(integer = 10, fraction = 2) BigDecimal amount
    ) {}

    private final OrderService service;
    private final OrderRepository orders;

    public OrderController(OrderService service, OrderRepository orders) {
        this.service = service;
        this.orders = orders;
    }

    /**
     * 202 for a new order (the saga runs asynchronously), 200 with the original order for
     * a repeated Idempotency-Key. Clients poll GET /orders/{id} for the outcome.
     */
    @PostMapping
    public ResponseEntity<?> place(
        @RequestHeader(value = "Idempotency-Key", required = false) String key,
        @Valid @RequestBody PlaceOrder body
    ) {
        if (key == null || key.isBlank() || key.length() > 128) {
            return ResponseEntity.badRequest().body(Map.of("error", "Idempotency-Key header (1-128 chars) is required"));
        }
        var placed = service.place(key, body.customerId(), body.productId(), body.quantity(), body.amount());
        var status = placed.created() ? HttpStatus.ACCEPTED : HttpStatus.OK;
        return ResponseEntity.status(status)
            .location(URI.create("/orders/" + placed.order().id()))
            .body(placed.order());
    }

    @GetMapping("/{id}")
    public ResponseEntity<Order> get(@PathVariable UUID id) {
        return ResponseEntity.of(orders.find(id));
    }
}
