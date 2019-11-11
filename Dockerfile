FROM maven:3.5-jdk-8-alpine as builder

WORKDIR /app
COPY . .

RUN mvn package -DskipTests

FROM adoptopenjdk/openjdk8:jdk8u202-b08-alpine-slim AS cloudrun

COPY --from=builder /app/cauldron-cloudrun-worker/target/cauldron-cloudrun-worker-*.jar /lib/cauldron-cloudrun-worker.jar

ENV PORT 80

CMD ["java","-Djava.security.egd=file:/dev/./urandom","-Dserver.port=${PORT}","-cp","/lib/*", "tech.cae.cauldron.cloudrun.worker.WorkerMain"]

FROM adoptopenjdk/openjdk8:jdk8u202-b08-alpine-slim AS worker

COPY --from=builder /app/cauldron-worker/target/cauldron-worker-*.jar /lib/cauldron-worker.jar

CMD ["java","-cp","/lib/*", "tech.cae.cauldron.worker.CauldronWorker"]
