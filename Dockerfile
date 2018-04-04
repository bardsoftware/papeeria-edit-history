FROM openjdk:8-slim

ARG COSMAS_VERSION

ADD ./build/distributions/${COSMAS_VERSION}.zip  ./${COSMAS_VERSION}.zip

RUN unzip -n ${COSMAS_VERSION}.zip

WORKDIR ./${COSMAS_VERSION}/bin/

ENTRYPOINT ./cosmas-server