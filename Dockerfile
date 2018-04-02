FROM openjdk:8-slim

ENV DIR_NAME Cosmas-1.0-SNAPSHOT

ADD ./build/distributions/${DIR_NAME}.zip  ./${DIR_NAME}.zip

RUN unzip -n ${DIR_NAME}.zip
ENTRYPOINT ./${DIR_NAME}/bin/cosmas-server