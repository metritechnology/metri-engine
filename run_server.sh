#!/bin/bash
if [ -f .env.local ]; then
  export $(grep -v '^#' .env.local | xargs)
fi
export RUST_LOG=info
cargo run --bin bootstrap
