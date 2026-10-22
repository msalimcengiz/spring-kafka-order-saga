package com.msalimcengiz.saga.order;

import com.msalimcengiz.saga.order.OrderSaga.Status;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record Order(
    UUID id,
    String customerId,
    String productId,
    int quantity,
    BigDecimal amount,
    Status status,
    String cancelReason,
    Instant createdAt,
    Instant updatedAt
) {}
