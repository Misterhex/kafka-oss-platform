FROM apache/kafka:4.1.1
COPY --chmod=755 infra/connect/start.sh /opt/lab/start.sh
ENTRYPOINT ["/bin/bash","/opt/lab/start.sh"]
