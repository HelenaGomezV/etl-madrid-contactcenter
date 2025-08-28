# Usa docker compose v2
DC := docker compose
ENV_FILE := .env

.PHONY: help env up down logs ps restart clean destroy

help:
	@echo "Comandos:"
	@echo "  make env      -> crea .env con FERNET_KEY y password web (por defecto 'airflow')"
	@echo "  make up       -> levanta el stack (incluye airflow-init la primera vez)"
	@echo "  make down     -> detiene contenedores"
	@echo "  make logs     -> muestra logs de Airflow"
	@echo "  make ps       -> estado de servicios"
	@echo "  make restart  -> reinicia servicios"
	@echo "  make clean    -> down -v (elimina volúmenes)"
	@echo "  make destroy  -> clean + borra .env (¡pierdes credenciales!)"

env:
	@set -e; \
	if [ ! -f "$(ENV_FILE)" ]; then \
	  echo "Creando $(ENV_FILE) ..."; \
	  touch "$(ENV_FILE)"; \
	fi; \
	if ! grep -q '^_AIRFLOW_WWW_USER_USERNAME=' "$(ENV_FILE)"; then \
	  echo "_AIRFLOW_WWW_USER_USERNAME=airflow" >> "$(ENV_FILE)"; \
	fi; \
	if ! grep -q '^_AIRFLOW_WWW_USER_PASSWORD=' "$(ENV_FILE)"; then \
	  echo "_AIRFLOW_WWW_USER_PASSWORD=airflow" >> "$(ENV_FILE)"; \
	fi; \
	echo "Listo. Variables escritas en $(ENV_FILE)."

up: env
	$(DC) --env-file $(ENV_FILE) up -d

down:
	$(DC) --env-file $(ENV_FILE) down

logs:
	$(DC) --env-file $(ENV_FILE) logs -f airflow-webserver

ps:
	$(DC) --env-file $(ENV_FILE) ps

restart:
	$(DC) --env-file $(ENV_FILE) restart

clean:
	$(DC) --env-file $(ENV_FILE) down -v

destroy: clean
	@rm -f $(ENV_FILE)
	@echo "Eliminado $(ENV_FILE)."
