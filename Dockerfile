FROM maven:3.5-jdk-8-alpine as builder

ARG BUILD_VERSION
ENV BUILD_VERSION ${BUILD_VERSION:-dev}

WORKDIR /app
COPY . .

RUN mvn versions:set -DnewVersion=$BUILD_VERSION -DprocessAllModules=true && mvn clean package -DskipTests

FROM adoptopenjdk/openjdk8:jdk8u202-b08-alpine-slim AS cloudrun

COPY --from=builder /app/cauldron-cloudrun-worker/target/*.jar /lib/

ENV PORT 80

EXPOSE ${PORT}

CMD ["java","-Djava.security.egd=file:/dev/./urandom","-cp","/lib/*", "tech.cae.cauldron.cloudrun.worker.WorkerMain"]

FROM adoptopenjdk/openjdk8:jdk8u202-b08-alpine-slim AS worker

COPY --from=builder /app/cauldron-worker/target/*.jar /lib/

CMD ["java","-cp","/lib/*", "tech.cae.cauldron.worker.CauldronWorker"]
