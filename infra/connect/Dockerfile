FROM maven:3.9.9-eclipse-temurin-21 AS build
ENV MAVEN_OPTS="-Xmx256m -XX:ActiveProcessorCount=2"
WORKDIR /src
COPY pom.xml .
COPY common/pom.xml common/pom.xml
COPY apps/pom.xml apps/pom.xml
COPY controller/pom.xml controller/pom.xml
COPY tools/pom.xml tools/pom.xml
COPY schema-backup/pom.xml schema-backup/pom.xml
COPY connect-transforms connect-transforms
RUN mvn -B -ntp -pl connect-transforms -am verify
FROM apache/kafka:4.1.1
COPY --from=build /src/connect-transforms/target/connect-transforms-1.0.0.jar /opt/kafka/libs/lab-connect-transforms.jar
COPY --chmod=755 infra/connect/start.sh /opt/lab/start.sh
ENTRYPOINT ["/bin/bash","/opt/lab/start.sh"]
