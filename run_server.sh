#!/bin/bash
export STUB_DYNAMODB=1
export RUST_LOG=debug
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
cargo run --bin bootstrap
