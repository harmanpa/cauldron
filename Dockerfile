
FROM adoptopenjdk/openjdk8:jdk8u202-b08-alpine-slim AS cloudrun

COPY ./cauldron-cloudrun-worker/target/*.jar /lib/

ENV PORT 80

EXPOSE ${PORT}

CMD ["java","-Djava.security.egd=file:/dev/./urandom","-cp","/lib/*", "tech.cae.cauldron.cloudrun.worker.WorkerMain"]

FROM adoptopenjdk/openjdk8:jdk8u202-b08-alpine-slim AS worker

COPY ./cauldron-worker/target/*.jar /lib/

CMD ["java","-cp","/lib/*", "tech.cae.cauldron.worker.CauldronWorker"]
