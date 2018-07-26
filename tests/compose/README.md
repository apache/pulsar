This are docker compose files to quickly bring up a pulsar
cluster. They use the pulsar testing docker image. To generate this,
run ```mvn install -DskipTests -Pdocker``` in the top-level directory
of the project.

To run, change directory into multi or simple, and run:
```
docker-compose up
```
