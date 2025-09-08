### Infrastructure for Local Development

This directory contains infrastructure configurations needed to run this service locally,
which includes the following services:
- Kafka cluster

### Setup 

First navigate to the `infra` directory and spin up the docker container
```agsl
cd infra
# Spin up the Kafka cluster along with Zookeeper and kafka UI
docker-compose up
```
Once the services are up and running,
Kafka UI will be available at `http://localhost:8080` and Kafka broker will be available at `localhost:9092`.

### Volumes
- The Kafka data is persisted in `kafka_data`.
- The Zookeeper data is persisted in `zookeeper_data` and `zookeeper_log`.

### Pause and Resume
To pause the containers without removing data inside the queue, run
```agsl
docker-compose stop
```

### Teardown
To stop and remove the containers, networks, and volumes created by `docker-compose up`, run
```agsl
docker-compose down -v
```
The `-v` flag ensures that the associated volumes are also removed.