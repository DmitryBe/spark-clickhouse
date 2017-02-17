
# docker images params
REPO=dmitryb/clickhouse-spark-connector
TAG=0.0.1

build:
	sbt compile
	sbt pack

pack:
	sbt pack-archive

run:
	env JAVA_OPTS="-Xmx4g -Xms4g -server -XX:+UseParallelGC -XX:NewRatio=1" \
	./target/pack/bin/main --conf

start-activator:
	./bin/activator ui -Dhttp.address=0.0.0.0 -Dhttp.port=8088

docker-build:
	docker build -t $(REPO):$(TAG) .

docker-push:
	docker push $(REPO):$(TAG)

docker-clean:
	docker rm $(docker ps -a -q)
	docker rmi $(docker images | grep "dmitryb/clickhouse-spark-connector" | awk "{print $3}")

# to create fat jar (not used)
assembly:
	sbt assembly

dev-local:
	#sbt clean compile
	#sbt pack
	mkdir -p target/l
	cp -f target/pack/lib/clickhouse* target/l/
	cp -f target/pack/lib/guava* target/l/

clickhouse-server-start:
	docker run -it -d --name clickhouse-server -p 8123:8123 -v `pwd`/clickhouse_files/config.xml:/etc/clickhouse-server/config.xml yandex/clickhouse-server