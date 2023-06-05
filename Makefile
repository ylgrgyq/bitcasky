VERSION := $(shell cat VERSION)
RUSTV=stable
DOCKER_TAG=$(VERSION)
GITHUB_TAG=v$(VERSION)
GIT_COMMIT=$(shell git rev-parse HEAD)
TARGET_LINUX=x86_64-unknown-linux-musl
TARGET_DARWIN=x86_64-apple-darwin
TEST_BUILD=$(if $(RELEASE),release,debug)
CLIENT_LOG=warn
TEST_LOG=warn
SKIP_CHECK=--skip-checks

build-all-test:
	cargo build --lib --tests --all-features

run-all-unit-test: 
	cargo test --lib --all-features

run-all-doc-test:
	cargo test --all-features --doc

install-clippy:
	rustup component add clippy --toolchain $(RUSTV)

check-clippy:	install-clippy
	cargo +$(RUSTV) clippy --all --all-targets --all-features --tests -- -D warnings -A clippy::upper_case_acronyms

