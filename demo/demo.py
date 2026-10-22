"""Guided demo of spring-kafka-order-saga. Run with `make demo`; demo/run.sh stops the
payment service and injects a poison record between the phases."""

import sys
import uuid
from decimal import Decimal

from lib import done, http, next_step, note, ok, show, step, wait_until

ORDERS = "http://order-service:8080"
PAYMENTS = "http://payment-service:8080"
INVENTORY = "http://inventory-service:8080"
FINAL = ("COMPLETED", "CANCELLED")


def place(customer: str, product: str, qty: int, amount: str, key: str | None = None):
    return http("POST", f"{ORDERS}/orders",
                {"customerId": customer, "productId": product, "quantity": qty, "amount": amount},
                {"Idempotency-Key": key or str(uuid.uuid4())})


def order(id_: str) -> dict:
    return http("GET", f"{ORDERS}/orders/{id_}").json()


def finished(id_: str, timeout: float = 30) -> dict:
    return wait_until(lambda: order(id_), lambda o: o["status"] in FINAL, timeout=timeout, every=0.3)


def balance(customer: str) -> Decimal:
    return Decimal(str(http("GET", f"{PAYMENTS}/accounts/{customer}").json()["balance"]))


def stock(product: str) -> int:
    return http("GET", f"{INVENTORY}/products/{product}").json()["available"]


def basics() -> None:
    step("Happy path: order → charge payment → reserve stock → COMPLETED, all over Kafka")
    b0, s0 = balance("c001"), stock("p001")
    r = place("c001", "p001", 2, "149.90")
    show(r)
    ok(r.status == 202 and r.json()["status"] == "PAYMENT_PENDING", "202: order stored with its first command in the outbox")
    o = finished(r.json()["id"])
    ok(o["status"] == "COMPLETED", "saga completed")
    ok(balance("c001") == b0 - Decimal("149.90") and stock("p001") == s0 - 2,
       f"c001 balance {b0} → {balance('c001')}, p001 stock {s0} → {stock('p001')}")

    step("A client retry with the same Idempotency-Key returns the same order, nothing is charged twice")
    key = f"checkout-{uuid.uuid4()}"
    first = place("c002", "p002", 1, "50.00", key)
    second = place("c002", "p002", 1, "50.00", key)
    ok(second.status == 200 and second.json()["id"] == first.json()["id"], "200 with the original order")
    finished(first.json()["id"])
    ok(balance("c002") == Decimal("4950.00"), "c002 charged exactly once (5000.00 → 4950.00)")

    step("Not enough money: payment is declined and the order is cancelled; no compensation is needed")
    r = place("c003", "p003", 1, "6000.00")
    o = finished(r.json()["id"])
    note(f"status {o['status']}, reason: {o['cancelReason']}")
    ok(o["status"] == "CANCELLED" and balance("c003") == Decimal("5000.00"), "cancelled, balance untouched")

    step("Out of stock after payment: the saga compensates by refunding, then cancels")
    finished(place("c004", "p020", 100, "100.00").json()["id"])  # takes the last 100 units of p020
    b0 = balance("c005")
    r = place("c005", "p020", 1, "75.00")
    states = []
    o = wait_until(lambda: (states.append(order(r.json()["id"])["status"]) or order(r.json()["id"])),
                   lambda o: o["status"] in FINAL, timeout=30, every=0.1)
    note(f"states seen: {' → '.join(dict.fromkeys(states))}")
    note(f"reason: {o['cancelReason']}")
    payment = http("GET", f"{PAYMENTS}/payments/{r.json()['id']}").json()
    ok(o["status"] == "CANCELLED" and payment["status"] == "REFUNDED", "payment charged and then REFUNDED")
    ok(balance("c005") == b0 and stock("p020") == 0, f"c005 balance back to {b0}, stock still 0 (never negative)")

    step("Invalid requests are rejected before anything is stored")
    r = http("POST", f"{ORDERS}/orders", {"customerId": "c001", "productId": "p001", "quantity": 1, "amount": "10.00"})
    ok(r.status == 400, "400 without an Idempotency-Key")
    r = place("c001", "p001", 0, "-5")
    ok(r.status == 400, "400 for quantity 0 and a negative amount")


def outage_place() -> None:
    step("payment-service is down: orders are still accepted, the command waits in Kafka")
    r = place("c006", "p006", 1, "20.00")
    ok(r.status == 202, "202 Accepted")
    o = wait_until(lambda: order(r.json()["id"]), lambda o: True, timeout=1)
    ok(o["status"] == "PAYMENT_PENDING", "order waits in PAYMENT_PENDING")
    open("/tmp/outage_order", "w").write(r.json()["id"])


def outage_resume() -> None:
    step("payment-service is back: it consumes the waiting command and the saga finishes")
    o = finished(open("/tmp/outage_order").read(), timeout=60)
    ok(o["status"] == "COMPLETED", "completed without any client retry")


def poison_check() -> None:
    step("Consumers keep working after the poison record (the same-partition case is covered by PaymentServiceIT)")
    o = finished(place("c007", "p007", 1, "10.00").json()["id"])
    ok(o["status"] == "COMPLETED", "a new order completes; nothing is stuck")
    metrics = http("GET", f"{ORDERS}/actuator/prometheus").body.decode().splitlines()
    for line in metrics:
        if line.startswith("saga_transitions_total"):
            note(line)
    done("happy path, idempotency, decline, compensation (refund), validation, a participant outage and a poison record.")


{"basics": basics, "outage_place": outage_place, "outage_resume": outage_resume, "poison_check": poison_check}[sys.argv[1]]()
with open("/tmp/next_step", "w") as f:
    f.write(str(next_step()))
