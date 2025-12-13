# follow below steps if you have build jar files 
# ----------------------------------------------------------- 
    # FROM alfresco/alfresco-content-repository-community:25.3.0
    
    # COPY repo-extension/target/upload-events-extension-1.0.0.jar \
    #   /usr/local/tomcat/webapps/alfresco/WEB-INF/lib/
# ----------------------------------------------------------- 


# -------- Stage 1: Build extension --------
FROM maven:3.9.9-eclipse-temurin-17 AS builder
WORKDIR /build
COPY repo-extension /build
RUN mvn clean package -DskipTests

# -------- Stage 2: Alfresco runtime --------
FROM alfresco/alfresco-content-repository-community:25.3.0

# Copy JAR from builder stage into Alfresco
COPY --from=builder \
    /build/target/upload-events-extension-1.0.0.jar \
    /usr/local/tomcat/webapps/alfresco/WEB-INF/lib/
