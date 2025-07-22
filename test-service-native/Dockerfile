FROM amazoncorretto:24@sha256:e749d7b881e6619126be60e9dffd39b9230c0fa025261c8e86dbf10ab2fac1c7
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
