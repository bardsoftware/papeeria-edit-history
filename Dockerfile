FROM bardsoftware/papeeria:java8

ARG COSMAS_VERSION=Cosmas-1.0-SNAPSHOT

ADD ./build/distributions/${COSMAS_VERSION}.zip  /${COSMAS_VERSION}.zip

RUN unzip -n ${COSMAS_VERSION}.zip

RUN apt-get -qq update && apt-get -qqy install file

RUN wget -qO- https://get.docker.com/ | sh

RUN sudo usermod -aG docker $(whoami)

WORKDIR /${COSMAS_VERSION}/bin/

ENTRYPOINT ./cosmas-server