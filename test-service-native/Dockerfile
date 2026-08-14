FROM amazoncorretto:26@sha256:dc98b5a706a11c48143f54cd7e552a6ea10ea5fef12b9331dd8ac79711201655
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
