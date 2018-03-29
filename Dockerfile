FROM openjdk:8-jdk

ENV FILE_NAME distZip

ADD ./build/distributions/${FILE_NAME}.jar  ./${FILE_NAME}.jar

ENTRYPOINT java -jar ${FILE_NAME}.jar 
