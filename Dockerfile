FROM maven:3.9.9-eclipse-temurin-21 AS build
ENV MAVEN_OPTS="-Xmx256m -XX:ActiveProcessorCount=2"
WORKDIR /src
COPY pom.xml .
COPY common common
COPY apps apps
COPY controller controller
COPY tools tools
COPY schema-backup schema-backup
RUN mvn -B -ntp verify
FROM apache/kafka:4.1.1
USER root
ARG MODULE
COPY --from=build /src/${MODULE}/target/${MODULE}-1.0.0.jar /app/app.jar
USER appuser
ENTRYPOINT ["java","-Xms64m","-Xmx256m","-jar","/app/app.jar"]
