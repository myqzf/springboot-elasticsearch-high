package com.elastic.config;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchConfig implements FactoryBean<RestHighLevelClient>, InitializingBean, DisposableBean {
	private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchConfig.class);

	@Value("${spring.data.elasticsearch.cluster-nodes}")
	private String clusterNodes;

	private RestHighLevelClient restHighLevelClient;

	@Override
	public void afterPropertiesSet() throws Exception {
		restHighLevelClient = buildClient();
	}

	private RestHighLevelClient buildClient() {
		try {
			final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
			credentialsProvider.setCredentials(AuthScope.ANY,
			        new UsernamePasswordCredentials("elastic", "ARS22mBM9YIrxFBGD4pO"));
			
			RestClientBuilder builder = RestClient.builder(new HttpHost(clusterNodes.split(":")[0], Integer.parseInt(clusterNodes.split(":")[1]),"http"))
	        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
	            @Override
	            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
	                httpClientBuilder.disableAuthCaching(); 
	                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
	            }
	        });
			
			restHighLevelClient = new RestHighLevelClient(builder);
			
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		}
		return restHighLevelClient;
	}

	@Override
	public RestHighLevelClient getObject() throws Exception {
		return restHighLevelClient;
	}

	@Override
	public Class<?> getObjectType() {
		return RestHighLevelClient.class;
	}

	@Override
	public void destroy() throws Exception {
		try {
			if (restHighLevelClient != null) {
				restHighLevelClient.close();
			}
		} catch (final Exception e) {
			LOGGER.error("Error closing ElasticSearch client: ", e);
		}
	}
	
	@Bean(name = "cmsElasticSearchIndex")
	public String cmsElasticSearchIndex() throws Exception {
		String cmsIndex= "cms";
		GetIndexRequest getRequest = new GetIndexRequest();
		getRequest.indices(cmsIndex);
		boolean exists = restHighLevelClient.indices().exists(getRequest);
		if(!exists) {
			CreateIndexRequest request = new CreateIndexRequest(cmsIndex);
			CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request);
			boolean acknowledged = createIndexResponse.isAcknowledged(); // 是否所有节点都已确认请求
			LOGGER.info("是否所有节点都已确认请求?" + acknowledged);
		}
		return cmsIndex;
	}
}
