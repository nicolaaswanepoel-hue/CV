ENV?=.env
COMPOSE_CORE=docker/compose.core.yml
COMPOSE_KAFKA=docker/compose.kafka.yml

up:
\tdocker compose -f $(COMPOSE_CORE) --env-file $(ENV) up -d
down:
\tdocker compose -f $(COMPOSE_CORE) down -v
logs:
\tdocker compose -f $(COMPOSE_CORE) logs -f --tail=200
kafka-up:
\tdocker compose -f $(COMPOSE_CORE) -f $(COMPOSE_KAFKA) --env-file $(ENV) up -d
kafka-down:
\tdocker compose -f $(COMPOSE_CORE) -f $(COMPOSE_KAFKA) down -v
mkdocs-build:
\tmkdocs build
