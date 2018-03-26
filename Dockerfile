FROM openjdk:8-jdk

ENV FILE_NAME distZip

ADD ./build/distributions/${FILE_NAME}.zip  ./${FILE_NAME}.zip

RUN unzip ${FILE_NAME}.zip

ENTRYPOINT java ${FILE_NAME}
