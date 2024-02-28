FROM ubuntu:jammy-20220421

ENV WORKDIR /root
WORKDIR $WORKDIR

RUN apt-get update

RUN DEBIAN_FRONTEND=noninteractive \
    apt-get install -y \
        default-jdk \
        maven \
        vim

COPY ./src $WORKDIR/montecristo/src
COPY pom.xml $WORKDIR/montecristo/pom.xml

WORKDIR montecristo
RUN mvn clean package
WORKDIR $WORKDIR

COPY docker-entrypoint.sh $WORKDIR
ENTRYPOINT ["/root/docker-entrypoint.sh"]
