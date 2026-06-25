# Multi-stage build for optimal image size

# Stage 1: Build the application
FROM eclipse-temurin:25-jdk AS build

WORKDIR /app

# Copy Gradle wrapper and build configuration first (cached layer)
COPY gradlew .
COPY gradle gradle
COPY settings.gradle build.gradle ./

# Pre-download dependencies to leverage Docker layer caching
RUN ./gradlew --no-daemon dependencies > /dev/null 2>&1 || true

# Copy source code
COPY src ./src

# Build the application
RUN ./gradlew --no-daemon clean bootJar -x test

# Stage 2: Runtime image
FROM eclipse-temurin:25-jre-alpine

WORKDIR /app

# Copy the built jar from the build stage
COPY --from=build /app/build/libs/GenericConsumer-0.0.1-SNAPSHOT.jar app.jar

# Expose the default Spring Boot port
EXPOSE 8080

# Set JVM options for containerized environment
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
