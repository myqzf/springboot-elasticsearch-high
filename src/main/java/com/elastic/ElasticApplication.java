package com.elastic;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;


@SpringBootApplication(scanBasePackages = "com.elastic")
public class ElasticApplication {

	@Autowired
	private RestHighLevelClient restHighLevelClient;
	
	@Bean
	public ObjectMapper objectMapper() {
		ObjectMapper om = new ObjectMapper();
		om.configure(Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
		om.configure(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS, false);
		om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//		final SimpleModule module = new SimpleModule("topcloud", new Version(1, 0, 0, "", "", ""));
//		om.registerModule(module);
		return om;
	}
	
	@Bean(name = "cmsElasticSearchIndex")
	public  String cmsElasticSearchIndex() throws Exception {
		String cmsIndex= "cms";
		GetIndexRequest getRequest = new GetIndexRequest();
		getRequest.indices(cmsIndex);
		boolean exists = restHighLevelClient.indices().exists(getRequest);
		if(!exists) {
			CreateIndexRequest request = new CreateIndexRequest(cmsIndex);
			CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request);
			boolean acknowledged = createIndexResponse.isAcknowledged(); // 是否所有节点都已确认请求
		}
		return cmsIndex;
	}
	
	public static void main(String[] args) {
		SpringApplication.run(ElasticApplication.class, args);
	}
}
