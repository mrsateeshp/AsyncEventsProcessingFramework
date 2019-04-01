docker run -d -p 2181:2181 -e ZOOKEEPER_CLIENT_PORT=2181 confluentinc/cp-zookeeper:4.1.0
docker run -d -p 9090:9090 -v prometheus-data:/prometheus -v /Users/spinnama/dev/github/AsyncEventsProcessingFramework/aepf/aepf-test-impl/src/test/resources/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus:v2.8.0
docker run -d -p 9095:9095 -v /Users/spinnama/dev/github/AsyncEventsProcessingFramework/aepf/aepf-test-impl/src/test/resources/application-docker.yml:/etc/config/application.yml mrsateeshp/aepf-test-impl:1.0.0-SNAPSHOT
docker run -d -p 9096:9095 -v /Users/spinnama/dev/github/AsyncEventsProcessingFramework/aepf/aepf-test-impl/src/test/resources/application-docker.yml:/etc/config/application.yml mrsateeshp/aepf-test-impl:1.0.0-SNAPSHOT
docker run -d -p 9097:9095 -v /Users/spinnama/dev/github/AsyncEventsProcessingFramework/aepf/aepf-test-impl/src/test/resources/application-docker.yml:/etc/config/application.yml mrsateeshp/aepf-test-impl:1.0.0-SNAPSHOT

query: rate(index_based_requests_total[15s])

docker volume create grafana-data

docker run \
  -d \
  -p 3000:3000 \
  -v grafana-data:/var/lib/grafana \
  grafana/grafana:6.0.2