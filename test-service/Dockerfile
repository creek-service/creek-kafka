FROM amazoncorretto:20@sha256:ac2d30e9fb485c8e6a5d9a60a24bb620dc7e9da56d66daa422ec8aa54c804e1d
ARG APP_NAME
ARG APP_VERSION
ENV VERSION=$APP_VERSION

RUN yum update -y
RUN yum install -y tar lsof

RUN mkdir -p /opt/creek

COPY bin /bin
COPY log4j /log

COPY ${APP_NAME}-${APP_VERSION}.tar /opt/creek
WORKDIR /opt/creek
RUN tar xf ${APP_NAME}-${APP_VERSION}.tar
RUN ln -s ${APP_NAME}-${APP_VERSION} service

ENTRYPOINT ["/bin/run.sh"]
