################################################################################
# H3 builder image
################################################################################
FROM centos:7.7.1908 as h3-builder

# Install dependencies
RUN yum groupinstall -y "Development Tools" && \
    yum install -y epel-release && \
    yum install -y cmake3 glib2-devel libuuid-devel hiredis-devel cppcheck fuse3 fuse3-devel python3-devel python3-wheel java-8-openjdk-headless maven && \
    yum clean all \
    && rm -rf /var/cache/yum \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

# Copy in H3, build, and install
COPY h3lib /root/h3lib/
COPY h3fuse /root/h3fuse/
COPY pyh3lib /root/pyh3lib/
COPY JH3lib /root/JH3lib/
COPY Jclouds-H3 /root/Jclouds-H3/

ARG BUILD_TYPE=Release

WORKDIR /root/h3lib
RUN mkdir build && \
    (cd build && cmake3 -DCMAKE_INSTALL_PREFIX="/usr" -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" .. && make package && make install)

WORKDIR /root/h3fuse
RUN mkdir build && \
    (cd build && cmake3 -DCMAKE_INSTALL_PREFIX="/usr" -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" .. && make package && make install)

WORKDIR /root/pyh3lib
RUN ./setup.py bdist_wheel && \
    pip3 install dist/pyh3lib-1.2-cp36-cp36m-linux_x86_64.whl

WORKDIR /root/JH3lib
RUN mvn clean install

WORKDIR /root/Jclouds-H3
RUN mvn clean install -DskipTests=true -Drat.skip=true -Dcheckstyle.skip=true

WORKDIR /root

####################################################################################################
# H3 distribution
####################################################################################################
FROM centos:7.7.1908 as h3

COPY --from=h3-builder /root/h3lib/build/h3lib*.rpm /root/
COPY --from=h3-builder /root/h3fuse/build/h3fuse*.rpm /root/
COPY --from=h3-builder /root/pyh3lib/dist/pyh3lib*.whl /root/
WORKDIR /root
RUN yum install -y epel-release && \
    yum install -y python3 fuse3 && \
    yum install -y h3lib*.rpm h3fuse*.rpm && \
    pip3 install pyh3lib*.whl && \
    rm -rf *.rpm *.whl && \
    yum clean all \
    && rm -rf /var/cache/yum \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

################################################################################
# S3proxy builder image
################################################################################
FROM maven:3.5.0-jdk-8-alpine as s3proxy-builder

COPY --from=h3-builder /root/.m2/ /root/.m2/

RUN mkdir /opt && \
    cd /opt && \
    curl -sLO https://github.com/gaul/s3proxy/archive/s3proxy-1.7.1.tar.gz && \
    tar -zxvf s3proxy-1.7.1.tar.gz && \
    mv s3proxy-s3proxy-1.7.1 s3proxy

COPY Jclouds-H3/config/s3proxy.conf /opt/s3proxy/
#COPY Jclouds-H3/config/keystore.jks /opt/s3proxy/
#COPY Jclouds-H3/config/run-docker-container.sh /opt/s3proxy/src/main/resources/

WORKDIR /opt/s3proxy
RUN sed -i 's/filesystem/h3/g' pom.xml && \
    mvn package -DskipTests

####################################################################################################
# S3proxy with Jclouds-H3 distribution
####################################################################################################
FROM h3-builder as h3-s3proxy

WORKDIR /opt/s3proxy

COPY --from=s3proxy-builder /opt/s3proxy/target/s3proxy /opt/s3proxy/
COPY Jclouds-H3/config/keystore.jks /opt/s3proxy/
COPY Jclouds-H3/config/run-docker-container.sh /opt/s3proxy/

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
