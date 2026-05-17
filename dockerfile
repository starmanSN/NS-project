FROM eclipse-temurin:17-jdk

# Аргумент для пути к jar — по умолчанию target/*.jar
ARG JAR_FILE=build/libs/*.jar

# Копирование jar файла
COPY ${JAR_FILE} app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app.jar"]