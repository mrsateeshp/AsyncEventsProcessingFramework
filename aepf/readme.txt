docker run -d -p 2181:2181 -e ZOOKEEPER_CLIENT_PORT=2181 confluentinc/cp-zookeeper:4.1.0
docker run -d -p 9090:9090 -v /Users/spinnama/dev/github/AsyncEventsProcessingFramework/aepf/aepf-test-impl/src/test/resources/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus
docker run -d -p 9095:9095 mrsateeshp/aepf-test-impl:1.0.0-SNAPSHOT
docker run -d -p 9096:9095 mrsateeshp/aepf-test-impl:1.0.0-SNAPSHOT
docker run -d -p 9097:9095 mrsateeshp/aepf-test-impl:1.0.0-SNAPSHOT

query: rate(index_based_requests_total[15s])