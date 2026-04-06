.PHONY: check test fmt clippy
all: check

test:
	cargo test --all-features

fmt:
	cargo fmt \
						--package constellation \
						--package links \
						--package pocket \
						--package quasar \
						--package slingshot \
						--package spacedust \
						--package ufos
	cargo +nightly fmt --package jetstream

clippy:
	cargo clippy --all-targets --all-features -- -D warnings

check: test fmt clippy
