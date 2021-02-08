################################################################################
# Builder image
################################################################################
FROM centos:7.7.1908 as h3-builder

# Install dependencies
RUN yum groupinstall -y "Development Tools" && \
    yum install -y epel-release && \
    yum install -y cmake3 glib2-devel libuuid-devel hiredis-devel cppcheck fuse3 fuse3-devel python3-devel python3-wheel && \
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

ARG BUILD_TYPE=Release

WORKDIR /root/h3lib
RUN mkdir build && \
    (cd build && cmake3 -DCMAKE_INSTALL_PREFIX="/usr" -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" .. && make package && make install)

WORKDIR /root/h3fuse
RUN mkdir build && \
    (cd build && cmake3 -DCMAKE_INSTALL_PREFIX="/usr" -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" .. && make package && make install)

WORKDIR /root/pyh3lib
RUN ./setup.py bdist_wheel && \
    pip3 install dist/pyh3lib-1.1-cp36-cp36m-linux_x86_64.whl

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
