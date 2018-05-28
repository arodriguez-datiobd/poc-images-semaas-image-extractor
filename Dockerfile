FROM openjdk:8

RUN apt-get update && apt-get install -y jq

ADD target/semaas-image-extractor-*-SNAPSHOT-jar-with-dependencies.jar /tmp/semaas-image-extractor.jar
COPY docker_entrypoint.sh /
RUN chmod +x /docker_entrypoint.sh

ENTRYPOINT ["/docker_entrypoint.sh"]