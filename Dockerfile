# One image per service: docker build --build-arg SERVICE=order-service .
FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /src
COPY pom.xml .
COPY common/pom.xml common/
COPY order-service/pom.xml order-service/
COPY payment-service/pom.xml payment-service/
COPY inventory-service/pom.xml inventory-service/
RUN mvn -B -q dependency:go-offline -DexcludeArtifactIds=common || true
COPY . .
ARG SERVICE
RUN mvn -B -q -DskipTests -pl ${SERVICE} -am package \
 && java -Djarmode=tools -jar ${SERVICE}/target/${SERVICE}-1.0.0.jar extract --layers --launcher --destination /app

FROM eclipse-temurin:25-jre-alpine
RUN addgroup -S app && adduser -S app -G app
WORKDIR /app
COPY --from=build /app/dependencies/ ./
COPY --from=build /app/spring-boot-loader/ ./
COPY --from=build /app/snapshot-dependencies/ ./
COPY --from=build /app/application/ ./
USER app
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
