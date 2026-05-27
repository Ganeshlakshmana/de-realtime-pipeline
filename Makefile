# ==============================================================================
#  ██████╗ ███████╗    ██████╗ ███████╗ █████╗ ██╗  ████████╗██╗███╗   ███╗███████╗
#  ██╔══██╗██╔════╝    ██╔══██╗██╔════╝██╔══██╗██║  ╚══██╔══╝██║████╗ ████║██╔════╝
#  ██████╔╝█████╗      ██████╔╝█████╗  ███████║██║     ██║   ██║██╔████╔██║█████╗
#  ██╔══██╗██╔══╝      ██╔══██╗██╔══╝  ██╔══██║██║     ██║   ██║██║╚██╔╝██║██╔══╝
#  ██║  ██║███████╗    ██║  ██║███████╗██║  ██║███████╗██║   ██║██║ ╚═╝ ██║███████╗
#  ╚═╝  ╚═╝╚══════╝    ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚══════╝╚═╝   ╚═╝╚═╝     ╚═╝╚══════╝
#
#  Taxi Events Real-Time Pipeline  •  Kafka → Spark Streaming → PostgreSQL
# ==============================================================================

# ------------------------------------------------------------------------------
# Load .env file if present (silently ignore if missing)
# ------------------------------------------------------------------------------
-include .env
export

# ------------------------------------------------------------------------------
# Defaults (overridden by .env or environment)
# ------------------------------------------------------------------------------
POSTGRES_USER ?= pipeline
POSTGRES_DB   ?= taxi_pipeline

# ------------------------------------------------------------------------------
# Terminal colours
# ------------------------------------------------------------------------------
RESET  := \033[0m
BOLD   := \033[1m
CYAN   := \033[36m
GREEN  := \033[32m
YELLOW := \033[33m
RED    := \033[31m
WHITE  := \033[97m

# ------------------------------------------------------------------------------
# .PHONY declarations
# ------------------------------------------------------------------------------
.PHONY: help setup up build start down clean logs logs-producer logs-spark \
        logs-kafka ps verify count kafka-peek restart-spark restart-producer

# ==============================================================================
# DEFAULT TARGET
# ==============================================================================

## help: (default) Print this help message
help:
	@printf "\n$(BOLD)$(CYAN)╔══════════════════════════════════════════════════════════════╗$(RESET)\n"
	@printf "$(BOLD)$(CYAN)║      Taxi Events Real-Time Pipeline  —  Available Targets      ║$(RESET)\n"
	@printf "$(BOLD)$(CYAN)╚══════════════════════════════════════════════════════════════╝$(RESET)\n\n"
	@printf "$(BOLD)$(WHITE)  %-22s %s$(RESET)\n" "Target" "Description"
	@printf "  $(CYAN)%-22s$(RESET) %s\n" "──────────────────────" "──────────────────────────────────────────────"
	@grep -E '^## [a-zA-Z_-]+:' $(MAKEFILE_LIST) | \
		sed 's/^## //' | \
		awk -F: '{ printf "  $(GREEN)%-22s$(RESET) %s\n", $$1, $$2 }'
	@printf "\n$(BOLD)$(YELLOW)  Variables (from .env or environment):$(RESET)\n"
	@printf "  $(CYAN)POSTGRES_USER$(RESET)  = $(WHITE)$(POSTGRES_USER)$(RESET)\n"
	@printf "  $(CYAN)POSTGRES_DB$(RESET)    = $(WHITE)$(POSTGRES_DB)$(RESET)\n"
	@printf "\n$(BOLD)$(YELLOW)  Tip:$(RESET) override via $(CYAN).env$(RESET) or $(CYAN)make verify POSTGRES_USER=myuser$(RESET)\n\n"

# ==============================================================================
# SETUP
# ==============================================================================

## setup: Generate test CSV data via scripts/generate_test_data.py
setup:
	@printf "$(BOLD)$(CYAN)▶ Generating test data...$(RESET)\n"
	python scripts/generate_test_data.py
	@printf "$(GREEN)✔ Test data generated.$(RESET)\n"

# ==============================================================================
# DOCKER COMPOSE LIFECYCLE
# ==============================================================================

## up: Build images and start all services in detached mode, then tail logs
up:
	@printf "$(BOLD)$(CYAN)▶ Building and starting all services...$(RESET)\n"
	docker compose up --build -d
	@printf "$(GREEN)✔ Services started. Tailing logs (Ctrl+C to stop)...$(RESET)\n"
	docker compose logs -f

## build: Build (or rebuild) Docker images without starting
build:
	@printf "$(BOLD)$(CYAN)▶ Building Docker images...$(RESET)\n"
	docker compose build
	@printf "$(GREEN)✔ Build complete.$(RESET)\n"

## start: Start all services in detached mode (no rebuild)
start:
	@printf "$(BOLD)$(CYAN)▶ Starting services (no rebuild)...$(RESET)\n"
	docker compose up -d
	@printf "$(GREEN)✔ Services started.$(RESET)\n"

## down: Stop and remove containers and networks
down:
	@printf "$(BOLD)$(YELLOW)▶ Stopping services...$(RESET)\n"
	docker compose down
	@printf "$(GREEN)✔ Services stopped.$(RESET)\n"

## clean: Stop and remove containers, networks, AND volumes (destructive!)
clean:
	@printf "$(BOLD)$(RED)▶ Tearing down all services and removing volumes...$(RESET)\n"
	docker compose down -v --remove-orphans
	@printf "$(GREEN)✔ Clean complete. All volumes removed.$(RESET)\n"

# ==============================================================================
# LOGS
# ==============================================================================

## logs: Tail logs for all services
logs:
	docker compose logs -f

## logs-producer: Tail logs for the producer service only
logs-producer:
	docker compose logs -f producer

## logs-spark: Tail logs for the Spark service only
logs-spark:
	docker compose logs -f spark

## logs-kafka: Tail logs for the Kafka service only
logs-kafka:
	docker compose logs -f kafka

# ==============================================================================
# STATUS
# ==============================================================================

## ps: Show status of all running services
ps:
	docker compose ps

# ==============================================================================
# DATABASE INSPECTION
# ==============================================================================

## verify: Query the last 20 aggregated taxi rows from PostgreSQL
verify:
	@printf "$(BOLD)$(CYAN)▶ Querying taxi_aggregates (last 20 rows)...$(RESET)\n"
	docker exec postgres psql \
		-U $${POSTGRES_USER} \
		-d $${POSTGRES_DB} \
		-c "SELECT window_start, window_end, pickup_location, trip_count, avg_fare \
		    FROM taxi_aggregates \
		    ORDER BY window_start DESC \
		    LIMIT 20;"

## count: Count total rows in taxi_aggregates
count:
	@printf "$(BOLD)$(CYAN)▶ Counting rows in taxi_aggregates...$(RESET)\n"
	docker exec postgres psql \
		-U $${POSTGRES_USER} \
		-d $${POSTGRES_DB} \
		-c "SELECT COUNT(*) AS total_rows FROM taxi_aggregates;"

# ==============================================================================
# KAFKA INSPECTION
# ==============================================================================

## kafka-peek: Peek at the last 5 messages on the taxi-events Kafka topic
kafka-peek:
	@printf "$(BOLD)$(CYAN)▶ Peeking at taxi-events topic (max 5 messages)...$(RESET)\n"
	docker exec kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic taxi-events \
		--from-beginning \
		--max-messages 5 \
		--timeout-ms 5000

# ==============================================================================
# SERVICE RESTARTS
# ==============================================================================

## restart-spark: Restart the Spark service
restart-spark:
	@printf "$(BOLD)$(CYAN)▶ Restarting Spark...$(RESET)\n"
	docker compose restart spark
	@printf "$(GREEN)✔ Spark restarted.$(RESET)\n"

## restart-producer: Restart the producer service
restart-producer:
	@printf "$(BOLD)$(CYAN)▶ Restarting producer...$(RESET)\n"
	docker compose restart producer
	@printf "$(GREEN)✔ Producer restarted.$(RESET)\n"
