# Build stage
FROM clojure:temurin-21-lein-alpine AS build
WORKDIR /app
COPY project.clj .
RUN lein deps
COPY . .
RUN lein uberjar

# Run stage
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /app/target/*-standalone.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
