FROM amazoncorretto:21@sha256:ead9110dc63e5c83427446bc3e6aebde4f6dc16f51726faa51374d3ca80db833
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
