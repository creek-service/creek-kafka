FROM amazoncorretto:19@sha256:848e933cdc4cf2311141de0c303379a425164e985b34af9ed5b16d5ad9a393b7
ARG APP_NAME
ARG APP_VERSION
ENV VERSION=$APP_VERSION

RUN yum update -y
RUN yum install -y tar lsof

RUN mkdir -p /opt/creek

COPY bin /bin
COPY log4j /log
COPY agent /opt/creek/agent

COPY ${APP_NAME}-${APP_VERSION}.tar /opt/creek
WORKDIR /opt/creek
RUN tar xf ${APP_NAME}-${APP_VERSION}.tar
RUN ln -s ${APP_NAME}-${APP_VERSION} service

ENTRYPOINT ["/bin/run.sh"]
