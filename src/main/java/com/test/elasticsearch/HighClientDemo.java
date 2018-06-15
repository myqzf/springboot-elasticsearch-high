package com.test.elasticsearch;

import java.io.IOException;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.xml.sax.helpers.DefaultHandler;

public class HighClientDemo {

	public static void main(String[] args) {
		//构建一个高级客户端，基于低级别客户端
		//可构建成与多个主机进行通信。
		RestClientBuilder builder = RestClient.builder(new HttpHost("47.98.128.106", 19200, "http"));
		RestHighLevelClient client = new RestHighLevelClient(builder);
	
		//设置每个请求需要发送的默认 Header
		Header[] defaultHeaders = new Header [] { new BasicHeader ("header" ,"value")};
		//设置在同一请求中进行多次尝试时应该遵守的超时时间。默认值是30秒。
		builder.setMaxRetryTimeoutMillis(10000);
		//设置一个监听，每次节点发生故障时都会收到通知，以防需要采取措施。
		builder.setFailureListener(new RestClient.FailureListener() {
			@Override
	    public void onFailure(HttpHost host) {
	        System.out.println(host.getHostName()+" 失败！");
	    }
		});
		//设置允许修改默认请求配置的回调（例如，请求超时，认证或 org.apache.http.client.config.RequestConfig.Builder 允许设置的内容。
		builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
	    @Override
	    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
	        return httpClientBuilder.setProxy(new HttpHost("proxy", 9000, "http"));  
	    }
	});
		
		//创建索引
		CreateIndexRequest request = new CreateIndexRequest("twitter");
	
		//设置分片数量和副本数量
		request.settings(Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas",2));
		
		request.mapping("tweet", 
		    "  {\n" +
		    "    \"tweet\": {\n" +
		    "      \"properties\": {\n" +
		    "        \"message\": {\n" +
		    "          \"type\": \"text\"\n" +
		    "        }\n" +
		    "      }\n" +
		    "    }\n" +
		    "  }", 
		    XContentType.JSON);
		
		//设置别名
		request.alias(
		    new Alias("twitter_alias")  
		);
		
		//任选
		request.timeout(TimeValue.timeValueMinutes(2)); //设置超时等待所有节点确认索引创建
		//request.timeout("2m");
		request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); //设置连接主节点超时
		//request.masterNodeTimeout("1m");
		request.waitForActiveShards(2); //在创建索引API返回响应之前等待的活动分片副本的数量
		//request.waitForActiveShards(ActiveShardCount.DEFAULT);
		
		//同步执行
//		CreateIndexResponse createIndexResponse = client.indices().create(request);
		
		//异步执行
		//如果执行成功，则使用ActionListener回调该onResponse方法，如果失败则使用onFailure方法。
		ActionListener<CreateIndexResponse> listener = new ActionListener<CreateIndexResponse>() {
	    @Override
	    public void onResponse(CreateIndexResponse createIndexResponse) {
	        System.out.println("执行成功");
	        boolean acknowledged = createIndexResponse.isAcknowledged(); //是否所有节点都已确认请求
	        boolean shardsAcknowledged = createIndexResponse.isShardsAcked();//是否在超时之前为索引中的每个分片启动了必需的分片副本数
	    }

	    @Override
	    public void onFailure(Exception e) {
	    	 System.out.println("执行失败");
	    }
		};
		client.indices().createAsync(request, listener);
		
		
		try {
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
