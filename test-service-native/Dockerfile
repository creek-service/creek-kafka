FROM amazoncorretto:26@sha256:f81e1cd3cb66130aa080f5ca35574b39e5980aac0882ecb05bf4d47cf6d0d017
ARG APP_NAME
ARG APP_VERSION
ENV VERSION=$APP_VERSION

RUN mkdir -p /opt/creek

COPY bin /bin
COPY log4j /log

COPY ${APP_NAME}-${APP_VERSION} /opt/creek/${APP_NAME}-${APP_VERSION}
WORKDIR /opt/creek
RUN ln -s ${APP_NAME}-${APP_VERSION} service

ENTRYPOINT ["/bin/run.sh"]
