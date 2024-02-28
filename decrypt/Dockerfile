FROM ubuntu:jammy-20220421

RUN apt-get update \
    && apt-get install -y \
        openssl

ENV WORKDIR /root
WORKDIR $WORKDIR
VOLUME $WORKDIR/artifacts

COPY docker-entrypoint.sh $WORKDIR/docker-entrypoint.sh

# must be hardcoded to use $1 correctly in docker-entrypoint.sh
ENTRYPOINT ["/root/docker-entrypoint.sh"]
