################################################################################
# H3 JDK builder image
################################################################################
ARG BASE=carvicsforth/h3:1.1
FROM ${BASE} as h3engine

RUN yum install -y java-8-openjdk-headless maven  && \
    yum clean all

# JH3lib build
WORKDIR /root/JH3lib
RUN mvn clean install

# H3Jclouds build
WORKDIR /root/Jclouds-H3
RUN mvn clean install -DskipTests=true -Drat.skip=true -Dcheckstyle.skip=true

WORKDIR /root

################################################################################
# S3proxy builder image
################################################################################
FROM maven:3.5.0-jdk-8-alpine as s3proxy-builder

COPY --from=h3engine /root/.m2/ /root/.m2/

RUN mkdir /opt && \
    cd /opt && \
    curl -sLO https://github.com/gaul/s3proxy/archive/s3proxy-1.7.1.tar.gz && \
    tar -zxvf s3proxy-1.7.1.tar.gz && \
    mv s3proxy-s3proxy-1.7.1 s3proxy

COPY config/s3proxy.conf /opt/s3proxy/
#COPY config/keystore.jks /opt/s3proxy/
#COPY config/run-docker-container.sh /opt/s3proxy/src/main/resources/

WORKDIR /opt/s3proxy
RUN sed -i 's/filesystem/h3/g' pom.xml && \
    mvn package -DskipTests

####################################################################################################
# S3proxy with Jclouds-H3 distribution
####################################################################################################
FROM h3engine as s3proxy-onH3

WORKDIR /opt/s3proxy

COPY --from=s3proxy-builder /opt/s3proxy/target/s3proxy /opt/s3proxy/
COPY config/keystore.jks /opt/s3proxy/
COPY config/run-docker-container.sh /opt/s3proxy/

ENV \
    LOG_LEVEL="info" \
    S3PROXY_AUTHORIZATION="aws-v2-or-v4" \
    S3PROXY_IDENTITY="test:tester" \
    S3PROXY_CREDENTIAL="testing" \
    S3PROXY_CORS_ALLOW_ALL="false" \
    S3PROXY_CORS_ALLOW_ORIGINS="" \
    S3PROXY_CORS_ALLOW_METHODS="" \
    S3PROXY_CORS_ALLOW_HEADERS="" \
    S3PROXY_KEYSTORE_PATH="keystore.jks" \
    S3PROXY_KEYSTORE_PASSWORD="CARVICS" \
    S3PROXY_IGNORE_UNKNOWN_HEADERS="false" \
    JCLOUDS_PROVIDER="h3" \
    S3PROXY_ENDPOINT="https://0.0.0.0:8080" \
    JCLOUDS_REGION="" \
    JCLOUDS_KEYSTONE_VERSION="" \
    JCLOUDS_BASEDIR="file:///data/h3container" \
    JCLOUDS_KEYSTONE_SCOPE="" \
    JCLOUDS_KEYSTONE_PROJECT_DOMAIN_NAME=""

EXPOSE 8080
VOLUME /data

RUN mkdir /data/h3container

ENTRYPOINT ["/opt/s3proxy/run-docker-container.sh"]
