FROM ubuntu:18.04

# perl is for shasum
# git is required for go modules
# gcc/libsasl2-dev/lsof are required because of the tests
RUN apt-get update && \
    apt-get -y install curl binutils perl git gcc libsasl2-dev lsof && \
    apt-get -y install supervisor && \
    rm -rf /var/lib/apt/lists/*

RUN : \
    && curl --location 'http://downloads.mongodb.org/linux/mongodb-linux-x86_64-3.0.15.tgz' --output /tmp/mongodb-linux-x86_64-3.0.15.tgz \
    && echo '5f68f248a52193ad583f2dbd1a589f3270956a4737a054fce88a84fcf316eeae  /tmp/mongodb-linux-x86_64-3.0.15.tgz' | shasum -a 512256 --check \
    && :

RUN : \
    && curl --location 'https://dl.google.com/go/go1.12.7.linux-amd64.tar.gz' --output /tmp/go1.12.7.linux-amd64.tar.gz \
    && echo '60bcd6b16e19126c057ce9a1a5b2c8c4ff86e21b9035a3192b97eece5d6d5fe6  /tmp/go1.12.7.linux-amd64.tar.gz' | shasum -a 512256 --check \
    && :

RUN : \
    && tar -C / -xzf /tmp/mongodb-linux-x86_64-3.0.15.tgz \
    && (cd / && ln -sf mongodb-linux-x86_64-3.0.15 mongodb) \
    && :
ENV MONGODB=/mongodb

RUN : \
    && tar -C / -xzf /tmp/go1.12.7.linux-amd64.tar.gz \
    && :
ENV GOPATH /go

# for scripts/test
ENV IN_DOCKER=1

COPY . /src
