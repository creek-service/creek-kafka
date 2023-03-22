FROM amazoncorretto:19@sha256:ec346fa782608946b65816d1429e17488f4c9296cbb42c5119eaa0e60df6aaf0
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
