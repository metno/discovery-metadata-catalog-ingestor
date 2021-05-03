FROM alpine:3.13
LABEL maintainer="no-reply@met.no"


RUN apk add --no-cache \
      ca-certificates \
      dumb-init \
      git \
      libxml2 \
      libxslt \
      # install lxml by package, since it takes forever to build
      py3-lxml \
      py3-pip \
      py3-wheel;

ADD dmci /src/install/dmci
ADD external /src/install/external
ADD LICENSE README.md *ini *toml *py *cfg *yaml /src/install

RUN pip3 install gunicorn
RUN pip3 install -e /src/install

EXPOSE 8000
VOLUME /workdir

ENTRYPOINT ["dumb-init", "--"]

CMD ["sh"]
