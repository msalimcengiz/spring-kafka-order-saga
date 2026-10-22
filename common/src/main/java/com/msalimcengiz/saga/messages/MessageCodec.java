package com.msalimcengiz.saga.messages;

import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/** JSON encoding of envelopes. Decoding failures are permanent and go to the DLT. */
@Component
public class MessageCodec {

    private final JsonMapper json;

    public MessageCodec(JsonMapper json) {
        this.json = json;
    }

    public String encode(Envelope envelope) {
        return json.writeValueAsString(envelope);
    }

    public Envelope decode(String value) {
        try {
            Envelope e = json.readValue(value, Envelope.class);
            if (e.messageId() == null || e.message() == null || e.message().orderId() == null) {
                throw new MalformedMessageException("envelope is missing messageId, message or orderId");
            }
            return e;
        } catch (JacksonException ex) {
            throw new MalformedMessageException("cannot decode message: " + ex.getOriginalMessage());
        }
    }

    /** Thrown for records that can never be processed; they skip retries and go to the DLT. */
    public static class MalformedMessageException extends RuntimeException {
        public MalformedMessageException(String message) {
            super(message);
        }
    }
}
