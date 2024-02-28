FROM ubuntu:jammy-20220421

RUN apt-get update

RUN DEBIAN_FRONTEND=noninteractive \
    apt-get install -y \
        groff \
        python3-dev \
        python3-pip \
        unzip

RUN pip install --upgrade awscli pip

ENV WORKDIR /root
WORKDIR $WORKDIR

COPY .aws/credentials $WORKDIR/.aws/credentials
COPY docker-entrypoint.sh $WORKDIR/docker-entrypoint.sh

ENTRYPOINT ["/root/docker-entrypoint.sh"]
