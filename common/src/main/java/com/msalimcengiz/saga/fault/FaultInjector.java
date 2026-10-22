package com.msalimcengiz.saga.fault;

import java.util.concurrent.ThreadLocalRandom;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Throws a transient failure on a configurable share of calls, to exercise the retry and
 * redelivery paths in tests and chaos runs. Disabled (rate 0) unless configured.
 */
@Component
public class FaultInjector {

    private final double rate;

    public FaultInjector(@Value("${saga.fault-injection.rate:0}") double rate) {
        this.rate = rate;
    }

    public void maybeFail(String where) {
        if (rate > 0 && ThreadLocalRandom.current().nextDouble() < rate) {
            throw new TransientFailure("injected failure in " + where);
        }
    }

    public static class TransientFailure extends RuntimeException {
        public TransientFailure(String message) {
            super(message);
        }
    }
}
