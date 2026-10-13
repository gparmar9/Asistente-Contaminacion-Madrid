# Atajos para trabajar en local. `make` (o `make help`) lista los comandos.
#
# Las recetas delegan en scripts/local.py, que arranca los servicios, espera a que respondan
# y los para todos con Ctrl+C. Así funcionan igual desde PowerShell, Git Bash o Linux.
# Requisitos: GNU Make (en Windows: `winget install ezwinports.make`), Python 3.12,
# Node >= 20.19 y Docker Desktop.
#
# Variables que se pueden cambiar al llamar, p. ej. `make frontend PY=env-pontia-ml/bin/python`:
#   PY      Python del proyecto (por defecto, el del venv de la raíz)
#   PYTHON  Python base para crear Agente/.venv

ifeq ($(OS),Windows_NT)
  # cmd.exe explícito: el `bash` del PATH de Windows suele ser el de WSL, no el de Git.
  SHELL := cmd.exe
  ruta = $(subst /,\,$(1))
  vpy = $(call ruta,$(1)/Scripts/python.exe)
  PYTHON ?= py -3.12
else
  ruta = $(1)
  vpy = $(1)/bin/python
  PYTHON ?= python3
endif

PY ?= $(call vpy,venv)
PY_AGENTE := $(call vpy,Agente/.venv)
LOCAL := $(PY) scripts/local.py

.DEFAULT_GOAL := help
.PHONY: help frontend dev db phoenix rag agente api-usuario web preguntar stop install install-agente install-rag test

## --- Levantar todo ------------------------------------------------------------------

help: ## Lista los comandos
	@$(PYTHON) scripts/local.py ayuda

frontend: ## Dashboard con datos: PostgreSQL + ApiUsuario + web (http://localhost:5173)
	@$(LOCAL) db api-usuario web

dev: ## Todo: PostgreSQL + Phoenix + RAG + Agente + ApiUsuario + web (chat incluido)
	@$(LOCAL) db phoenix rag agente api-usuario web

## --- Servicios sueltos --------------------------------------------------------------

db: ## PostgreSQL en Docker (queda levantado)
	@$(LOCAL) db

phoenix: ## Phoenix, trazas del agente en Docker (http://localhost:6006; queda levantado)
	@$(LOCAL) phoenix

rag: ## RAG (http://localhost:8010/docs)
	@$(LOCAL) rag

agente: ## RAG + Agente (http://localhost:8200/docs)
	@$(LOCAL) rag agente

api-usuario: ## PostgreSQL + ApiUsuario (http://localhost:8000/docs)
	@$(LOCAL) db api-usuario

web: ## Web (Vite); la API tiene que estar levantada aparte
	@$(LOCAL) web

preguntar: ## Pregunta al chat desde la terminal: make preguntar P="..." [SESION=id] [CRUDO=1]
	@$(LOCAL) preguntar

stop: ## Para PostgreSQL y Phoenix (los datos se conservan en sus volúmenes)
	docker compose --profile observabilidad stop db phoenix

## --- Instalar y probar --------------------------------------------------------------

install: ## Primera vez: dependencias del venv de la raíz y de la web
	$(PY) -m pip install -r requirements.txt
	npm --prefix Web ci

install-agente: ## Crea Agente/.venv con sus dependencias
	$(PYTHON) -m venv $(call ruta,Agente/.venv)
	$(PY_AGENTE) -m pip install -r Agente/requirements-dev.txt

install-rag: ## Dependencias del RAG en el venv de la raíz (torch: ~1-2 GB)
	$(PY) -m pip install -r requirements-rag.txt

test: ## Tests de la raíz, ApiUsuario, Agente y web
	$(PY) -m pytest tests/ --ignore=tests/test_integracion_db.py -q
	$(PY) -m pytest ApiUsuario/tests/ -q
	$(PY_AGENTE) -m pytest Agente/tests/ -q
	npm --prefix Web test
