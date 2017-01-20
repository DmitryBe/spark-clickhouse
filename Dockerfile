FROM p7hb/docker-spark:2.1.0

ARG CLICKHOUSE_LOGS=/var/log/clickhouse-server

ENV APP_DIR /app

RUN mkdir -p /etc/apt/sources.list.d && \
	apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4 && \
	echo "deb http://repo.yandex.ru/clickhouse/trusty stable main" | tee /etc/apt/sources.list.d/clickhouse.list && \
	apt-get -y update && \
	apt-get -y install clickhouse-server-common clickhouse-client && \
	mkdir -p ${CLICKHOUSE_LOGS} && \
	touch ${CLICKHOUSE_LOGS}/tmp

ADD docker_files/docker_start.sh /docker_start.sh

RUN mkdir -p ${APP_DIR}
WORKDIR ${APP_DIR}

# clickhouse config with cluster def
COPY /clickhouse_files/config.xml /etc/clickhouse-server/

COPY /target/pack/lib/clickhouse* ${APP_DIR}/lib/
COPY /target/pack/lib/guava* ${APP_DIR}/lib/
COPY Makefile ${APP_DIR}

ENTRYPOINT ["/docker_start.sh"]