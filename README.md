# springboot-elasticsearch-high
springboot与elasticsearch集成
 通过[Java High Level REST Client](https://www.elastic.co/guide/en/elasticsearch/client/java-rest/6.3/java-rest-high.html) 集成
 
ElasticSearch计划在7.0中将会弃用TransportClient，并在8.0中完全删除它。但是目前springboot最新正式版本为2.0.3，集成的[spring-data-elasticsearch](https://github.com/spring-projects/spring-data-elasticsearch)为3.0.7版本，还不支持ElasticSearch 6 版本。所以可以通过集成了解Java High Level REST Client。

项目环境：
springboot 2.0.3
elasticsearch-rest-high-level-client 6.3.0
