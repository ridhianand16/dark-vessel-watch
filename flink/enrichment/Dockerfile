# syntax=docker/dockerfile:1
# The Flink enrichment job image, used by the JobManager, the TaskManager and the one-shot
# submit service. Build context: flink/enrichment.
#
# Stage 1 builds the fat jar with Maven and runs the unit tests; a failing test fails the
# image build. Maven's repository is a BuildKit cache mount, so rebuilds (and `make
# test-flink`) only go online when pom.xml gains a dependency.
FROM maven:3.10.0-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -B -q dependency:go-offline
COPY src/ src/
RUN --mount=type=cache,target=/root/.m2 mvn -B package

FROM flink:1.20.5-scala_2.12-java17
COPY --from=build /src/target/enrichment.jar /opt/dvw/enrichment.jar
COPY docker/flinkctl.sh /opt/dvw/flinkctl.sh
# Checkpoints and savepoints live on a named volume mounted here. A new named volume takes
# the ownership of the image directory it is mounted over, so Flink (uid 9999) can write it.
RUN mkdir -p /flink-data/checkpoints /flink-data/savepoints \
    && chown -R flink:flink /flink-data \
    && chmod +x /opt/dvw/flinkctl.sh
