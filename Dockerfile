FROM maven:3.9-eclipse-temurin-17

# Install AWS CLI
RUN apt-get update && \
    apt-get install -y unzip && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

COPY common/target/*.jar .

RUN curl -o opentelemetry-javaagent.jar -L https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.29.0/opentelemetry-javaagent.jar

COPY run_app.sh .

RUN chmod +x run_app.sh

ENTRYPOINT ["bash", "run_app.sh"]
