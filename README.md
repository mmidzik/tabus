# Tabus

Simple examples of deduplicating & counting pub/sub messages with various backends

## Backend Stores:
* [x] Memory
* [x] Redis
* [ ] Bigtable
* [ ] FireStore

## TODO:
* [ ] Dev setup & tests w/ emulators/local redis
* [ ] Benchmarking tools

To run redis image: `docker run -d --name test-redis -p 6379:6379 redis`
