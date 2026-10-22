.PHONY: demo up down test bench chaos k8s-validate

demo:          ## Guided tour: fresh stack, 9 checked steps (first run builds images)
	./demo/run.sh

up:            ## order :8081, 2× payment, 2× inventory, Kafka (KRaft), Postgres
	docker compose up -d --build --wait

down:
	docker compose --profile observability down -v

test:          ## unit + Testcontainers integration tests (needs Docker)
	./mvnw-docker verify

bench:         ## throughput and end-to-end saga latency, no faults (results in bench/results)
	./bench/run.sh

chaos:         ## orders under load while services and the Kafka broker are SIGKILLed
	./chaos/run.sh

k8s-validate:
	docker run --rm -v $(CURDIR)/deploy/k8s:/k registry.k8s.io/kubectl:v1.34.1 kustomize /k > rendered.yaml
	docker run --rm -v $(CURDIR)/rendered.yaml:/r.yaml ghcr.io/yannh/kubeconform:latest -strict -summary /r.yaml
