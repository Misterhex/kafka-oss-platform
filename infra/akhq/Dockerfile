FROM tchiotludo/akhq:0.26.0 AS distribution
# Keep every Java runtime in this lab on Java 21, including the administration UI.
FROM apache/kafka:4.1.1
COPY --from=distribution /app/akhq.jar /app/akhq.jar
ENV MICRONAUT_CONFIG_FILES=/app/application.yml
ENTRYPOINT ["java", "-Xms64m", "-Xmx256m", "-jar", "/app/akhq.jar"]
