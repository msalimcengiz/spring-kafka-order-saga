package com.msalimcengiz.saga.messages;

import java.time.Instant;
import java.util.UUID;

/**
 * What goes on the wire. {@code messageId} identifies this delivery attempt's content:
 * a redelivered record keeps it, so consumers use it to drop duplicates.
 */
public record Envelope(UUID messageId, Instant occurredAt, Message message) {

    public static Envelope of(Message message) {
        return new Envelope(UUID.randomUUID(), Instant.now(), message);
    }
}
