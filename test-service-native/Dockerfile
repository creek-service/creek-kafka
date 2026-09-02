FROM amazoncorretto:26@sha256:2f180ed0eeb1258ce4c96edda216b7d47a36d1a4d7bc19d06fa1db0646c3d7f0
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
