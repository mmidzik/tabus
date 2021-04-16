# Tabus

Simple examples of deduplicating & counting pub/sub messages with various backends

Implementations strive to be 1) concurrently safe and 2) scalable up to 1,000 r/s

## Backend Stores:
* [x] Memory
* [x] Redis
* [ ] Bigtable
* [ ] FireStore

## TODO:
* [ ] Dev setup & tests w/ emulators/local redis
* [ ] Benchmarking tools

To run redis image: `docker run -d --name test-redis -p 6379:6379 redis`