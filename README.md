# Tabus

Simple examples of deduplicating & counting pub/sub messages with various backends

## Backend Stores:
* [x] Memory
* [x] Redis
* [x] Bigtable

## TODO:
* [ ] Dev setup & tests w/ emulators/local redis
* [ ] Benchmarking tools

### Running tests
- **Redis**: `docker run -d --name test-redis -p 6379:6379 redis` (or use miniredis in tests; no container needed).
- **Bigtable**: Unit tests use an in-process bttest server. For a real emulator: `gcloud beta emulators bigtable start`, set `BIGTABLE_EMULATOR_HOST=localhost:8086`, and create a table with column families `m` (messages) and `a` (attribute counts).
