# Makefile
# load and export .env
ifneq (,$(wildcard ./.env))
  include .env
  export
endif

init:
	pip install -r requirements.txt
.PHONY: init

db:
	datasette serve kv_store.db
.PHONY: db

pull:
	python3 main.py
.PHONY: pull

convert:
	python3 convert.py
.PHONY: convert

cleanup:
	rm -rf outputs/*
	rm -rf uploads/*
.PHONY: cleanup

mail:
	python3 mail.py
.PHONY: mail

full: pull convert mail
.PHONY: full
