FROM ubuntu:jammy-20220421

RUN apt-get update

RUN DEBIAN_FRONTEND=noninteractive \
    apt-get install -y \
        groff \
        python3 \
        python3-pip \
        unzip

RUN pip install --upgrade awscli pip

ENV WORKDIR /root
WORKDIR $WORKDIR
VOLUME $WORKDIR/artifacts

COPY env/secrets/aws.env $WORKDIR/.aws/credentials
COPY docker-entrypoint.sh $WORKDIR/docker-entrypoint.sh

# must be hardcoded to use $1 correctly in docker-entrypoint.sh
ENTRYPOINT ["/root/docker-entrypoint.sh"]
