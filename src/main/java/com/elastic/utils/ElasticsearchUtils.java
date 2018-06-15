package com.elastic.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONObject;

@Component
public class ElasticsearchUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchUtils.class);

	@Autowired
	private RestHighLevelClient restHighLevelClient;

	private static RestHighLevelClient client;

	@PostConstruct
	public void init() {
		client = this.restHighLevelClient;
	}

	/**
	 * 同步创建索引
	 *
	 * @param index
	 * @return
	 */
	public static boolean createIndexSync(String index) {
		if (!isIndexExist(index)) {
			LOGGER.info("Index is not exits!");
		}
		CreateIndexRequest request = new CreateIndexRequest(index);
		try {
			CreateIndexResponse createIndexResponse = client.indices().create(request);
			boolean acknowledged = createIndexResponse.isAcknowledged(); // 是否所有节点都已确认请求
			LOGGER.info("是否所有节点都已确认请求?" + acknowledged);
		} catch (IOException e) {
			LOGGER.error("创建索引失败！");
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * 异步创建索引
	 *
	 * @param index
	 * @return
	 */
	public static boolean createIndexASync(String index) {
		if (!isIndexExist(index)) {
			LOGGER.info("Index is not exits!");
		}
		CreateIndexRequest request = new CreateIndexRequest(index);
		ActionListener<CreateIndexResponse> listener = new ActionListener<CreateIndexResponse>() {
			@Override
			public void onResponse(CreateIndexResponse createIndexResponse) {
				boolean acknowledged = createIndexResponse.isAcknowledged(); // 是否所有节点都已确认请求
				boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();// 是否在超时之前为索引中的每个分片启动了必需的分片副本数
				LOGGER.info("创建索引执行成功! acknowledged: " + acknowledged + " shardsAcknowledged: " + shardsAcknowledged);
			}

			@Override
			public void onFailure(Exception e) {
				LOGGER.error("创建索引执行失败");
				e.printStackTrace();
			}
		};
		client.indices().createAsync(request, listener);
		LOGGER.info("执行建立成功");

		return true;
	}

	/**
	 * 判断索引是否存在
	 * 
	 * @param index
	 * @return
	 */
	public static boolean isIndexExist(String index) {
		GetIndexRequest request = new GetIndexRequest();
		request.indices(index);
		request.humanReadable(true);
		boolean exists = false;
		try {
			exists = client.indices().exists(request);
		} catch (IOException e) {
			LOGGER.error("查询文档是否存在失败！");
			e.printStackTrace();
		}
		return exists;
	}

	/**
	 * 判断文档是否存在
	 * 
	 * @param index
	 * @return
	 */
	public static boolean isDocumentExist(String index, String type, String id) {
		GetRequest getRequest = new GetRequest(index, type, id);
		getRequest.fetchSourceContext(new FetchSourceContext(false));
		getRequest.storedFields("_none_");
		boolean exists = false;
		try {
			exists = client.exists(getRequest);
		} catch (IOException e) {
			LOGGER.error("查询文档是否存在失败！");
			e.printStackTrace();
		}
		return exists;
	}

	/**
	 * 删除索引
	 *
	 * @param index
	 * @return
	 */
	public static boolean deleteIndex(String index) {
		Boolean isSuccess = true;
		if (!isIndexExist(index)) {
			return isSuccess;
		}
		DeleteIndexRequest request = new DeleteIndexRequest(index);
		try {
			DeleteIndexResponse deleteIndexResponse	= client.indices().delete(request);
			isSuccess = deleteIndexResponse.isAcknowledged();
			LOGGER.info("删除索引 " + index + "  成功!");
		} catch (IOException e) {
			LOGGER.info("删除索引失败: " + index);
			e.printStackTrace();
		}
		return isSuccess;
	}

	/**
	 * 数据添加，指定Id
	 *
	 * @param jsonObject 要增加的数据
	 * @param index  索引
	 * @param type   类型
	 * @param id     数据Id
	 * @return
	 */
	public static String addDocumnetForId(JSONObject jsonObject, String index, String type, String id) {

		IndexRequest indexRequest = new IndexRequest(index, type, id).source(jsonObject);
		IndexResponse indexResponse = new IndexResponse();
		try {
			indexResponse = client.index(indexRequest);
			LOGGER.info("addData response status:{},id:{}", indexResponse.status().getStatus(), indexResponse.getId());
		} catch (IOException e) {
			LOGGER.info("添加文档数据失败！");
			e.printStackTrace();
		}
		return indexResponse.getId();
	}
	
	/**
	 * 数据添加，id随机生成
	 *
	 * @param jsonObject 要增加的数据
	 * @param index  索引
	 * @param type   类型
	 * @return
	 */
	public static String addDocumnet(JSONObject jsonObject, String index, String type) {
		return addDocumnetForId(jsonObject, index, type, UUID.randomUUID().toString().replaceAll("-", "").toUpperCase());
	}
	
	/**
   * 通过Id删除数据(同步)
   *
   * @param index 索引
   * @param type  类型
   * @param id    数据Id
	 * @throws IOException 
   */
  public static void deleteDocumentByIdSync(String index, String type, String id) throws IOException {
  	 DeleteRequest deleteRequest = new DeleteRequest(index, type, id);
  	 DeleteResponse deleteResponse = new DeleteResponse();
		 deleteResponse = client.delete(deleteRequest);
     LOGGER.info("deleteDataById response status:{},id:{}", deleteResponse.status().getStatus(), deleteResponse.getId());
  }
  
  /**
   * 通过Id删除数据(异步)
   *
   * @param index 索引
   * @param type  类型
   * @param id    数据Id
	 * @throws IOException 
   */
  public static void deleteDocumentByIdAsync(String index, String type, String id){
  	  DeleteRequest deleteRequest = new DeleteRequest(index, type, id);
	  	ActionListener<DeleteResponse> listener = new ActionListener<DeleteResponse>() {
		    @Override
		    public void onResponse(DeleteResponse deleteResponse) {
		    	int status = deleteResponse.status().getStatus();
		    	String id = deleteResponse.getId();
		    	ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
		    	if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
		    	    //处理成功碎片数量少于总碎片数量的情况
		    		 LOGGER.info("deleteDataById response status:{},id:{},totalShard:{},successShard:{}", status, id, shardInfo.getTotal(),shardInfo.getSuccessful());
		    	}
		    	if (shardInfo.getFailed() > 0) {
		    	    for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
		    	        String reason = failure.reason(); //处理潜在的失败
		    	        LOGGER.info("deleteDataById response shardFaileReason:{}", reason);
		    	    }
		    	}
		    }
	
		    @Override
		    public void onFailure(Exception e) {
		    	LOGGER.info("删除文档执行失败");
		    	e.printStackTrace();
		    }
			};
		client.deleteAsync(deleteRequest, listener);
	 }
  
  /**
   * 通过ID 更新数据(同步)
   *
   * @param jsonObject 要增加的数据
   * @param index      索引，类似数据库
   * @param type       类型，类似表
   * @param id         数据ID
   * @return
   * @throws IOException 
   */
  public static void updateDataByIdSync(JSONObject jsonObject, String index, String type, String id) throws IOException {
      UpdateRequest updateRequest = new UpdateRequest(index, type, id).doc(jsonObject);
      client.update(updateRequest);
  }
  
  /**
   * 通过ID 更新数据(异步)
   *
   * @param jsonObject 要增加的数据
   * @param index      索引，类似数据库
   * @param type       类型，类似表
   * @param id         数据ID
   * @return
   * @throws IOException 
   */
  public static void updateDataByIdASync(JSONObject jsonObject, String index, String type, String id) throws IOException {
  	  UpdateRequest updateRequest = new UpdateRequest(index, type, id).doc(jsonObject);
	  	ActionListener<UpdateResponse> listener = new ActionListener<UpdateResponse>() {
		    @Override
		    public void onResponse(UpdateResponse updateResponse) {
		    	String index = updateResponse.getIndex();
		    	String type = updateResponse.getType();
		    	String id = updateResponse.getId();
		    	long version = updateResponse.getVersion();
		    	System.out.println("更新成功！version: "+ version);
		    	if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
		    	    
		    	} else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
		    	    
		    	} else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {
		    	    
		    	} else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
		    	    
		    	}
		    	
		    	//通过fetchSource方法启用源检索时，响应返回包含更新文档的数据：
		    	GetResult result = updateResponse.getGetResult(); 
		    	if (result.isExists()) {
		    	    String sourceAsString = result.sourceAsString(); 
		    	    Map<String, Object> sourceAsMap = result.sourceAsMap(); 
		    	    byte[] sourceAsBytes = result.source(); 
		    	    System.out.println("sourceAsString: "+ sourceAsString);
		    	} else {
		    	    
		    	}
		    	
		    	//检查分片失败
		    	ReplicationResponse.ShardInfo shardInfo = updateResponse.getShardInfo();
		    	if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
		    	    
		    	}
		    	if (shardInfo.getFailed() > 0) {
		    	    for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
		    	        String reason = failure.reason(); 
		    	    }
		    	}
		    }
	
		    	@Override
		      public void onFailure(Exception e) {
			    	System.out.println("更新文档执行失败");
			    	e.printStackTrace();
		      }
		  };
		client.updateAsync(updateRequest, listener);
	 }

  /**
   * 通过ID获取数据
   *
   * @param index  索引，类似数据库
   * @param type   类型，类似表
   * @param id     数据ID
   * @param includFields 需要显示的字段，逗号分隔（缺省为全部字段）
   * @return
   * @throws IOException 
   */
  public static Map<String, Object> searchDocumentById(String index, String type, String id, String includFields, String excludFields) throws IOException {

      GetRequest getRequest = new GetRequest(index, type, id);

      String[] includes = Strings.EMPTY_ARRAY;
      String[] excludes = Strings.EMPTY_ARRAY;
      if (StringUtils.isNotEmpty(includFields)) {
      	includes = includFields.split(",");
      }
      if(StringUtils.isNotEmpty(excludFields)) {
      	excludes = excludFields.split(",");
      }
      FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
      getRequest.fetchSourceContext(fetchSourceContext);//包含特定字段
      
      GetResponse getResponse = client.get(getRequest);
      return getResponse.getSource();
  }
  
  
  /**
   * 使用分词查询
   *
   * @param index          索引名称
   * @param type           类型名称,可传入多个type逗号分隔
   * @param startTime      开始时间
   * @param endTime        结束时间
   * @param size           文档大小限制
   * @param fields         需要显示的字段，逗号分隔（缺省为全部字段）
   * @param sortField      排序字段
   * @param matchPhrase    true 使用，短语精准匹配
   * @param highlightField 高亮字段
   * @param matchStr       过滤条件（xxx=111,aaa=222）
   * @return
   * @throws IOException 
   */
  public static List<Map<String, Object>> searchListData(String index, String type, long startTime, long endTime, Integer size, String fields, String sortField, boolean matchPhrase, String highlightField, String matchStr) throws IOException {

  	SearchRequest searchRequest = new SearchRequest(index.split(","));
  	SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
  	
      if (StringUtils.isNotEmpty(type)) {
      	searchRequest.types(type.split(","));
      }
      BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
      if (startTime > 0 && endTime > 0) {
          boolQuery.must(QueryBuilders.rangeQuery("processTime")//待了解
                  .format("epoch_millis")
                  .from(startTime)
                  .to(endTime)
                  .includeLower(true)
                  .includeUpper(true));
      }

      //搜索的的字段
      if (StringUtils.isNotEmpty(matchStr)) {
          for (String s : matchStr.split(",")) {
              String[] ss = s.split("=");
              if (ss.length > 1) {
                  if (matchPhrase == Boolean.TRUE) {
                      boolQuery.must(QueryBuilders.matchPhraseQuery(s.split("=")[0], s.split("=")[1]));
                  } else {
                      boolQuery.must(QueryBuilders.matchQuery(s.split("=")[0], s.split("=")[1]));
                  }
              }

          }
      }

      // 高亮（xxx=111,aaa=222）
      if (StringUtils.isNotEmpty(highlightField)) {
      	HighlightBuilder highlightBuilder = new HighlightBuilder();

          //highlightBuilder.preTags("<span style='color:red' >");//设置前缀
          //highlightBuilder.postTags("</span>");//设置后缀

          // 设置高亮字段
          highlightBuilder.field(highlightField);
          searchSourceBuilder.highlighter(highlightBuilder);
      }
      searchSourceBuilder.query(boolQuery);
      if (StringUtils.isNotEmpty(fields)) {
      	searchSourceBuilder.fetchSource(fields.split(","), null);
      }
      searchSourceBuilder.fetchSource(true);
      searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC)); //按综合匹配得分降序
      if (StringUtils.isNotEmpty(sortField)) {
      	searchSourceBuilder.sort(new FieldSortBuilder(sortField).order(SortOrder.ASC)); 
      }
      if (size != null && size > 0) {
      	searchSourceBuilder.size(size);
      }

      //打印的内容 可以在 Elasticsearch head 和 Kibana  上执行查询
      LOGGER.info("\n{}", searchSourceBuilder);

      SearchResponse searchResponse = client.search(searchRequest);
      long totalHits = searchResponse.getHits().totalHits;
      long length = searchResponse.getHits().getHits().length;

      LOGGER.info("共查询到[{}]条数据,处理数据条数[{}]", totalHits, length);
      if (searchResponse.status().getStatus() == 200) {
          // 解析对象
          return setSearchResponse(searchResponse, highlightField);
      }
      return null;
  }

  /**
   * 高亮结果集 特殊处理
   *
   * @param searchResponse
   * @param highlightField
   */
  private static List<Map<String, Object>> setSearchResponse(SearchResponse searchResponse, String highlightField) {
      List<Map<String, Object>> sourceList = new ArrayList<Map<String, Object>>();
      StringBuffer stringBuffer = new StringBuffer();
    	SearchHits hits = searchResponse.getHits();

      for (SearchHit searchHit : hits.getHits()) {
          searchHit.getSourceAsMap().put("id", searchHit.getId());
      		Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
          if (StringUtils.isNotEmpty(highlightField)) {
              Text[] text = searchHit.getHighlightFields().get(highlightField).getFragments();
              if (text != null) {
                  for (Text str : text) {
                      stringBuffer.append(str.string());
                  }
                  //遍历 高亮结果集，覆盖 正常结果集
                  searchHit.getSourceAsMap().put(highlightField, stringBuffer.toString());
              }
          }
          sourceList.add(sourceAsMap);
      }
      return sourceList;
  }

  /**
   * 使用分词查询,并分页
   *
   * @param index          索引名称
   * @param type           类型名称,可传入多个type逗号分隔
   * @param currentPage    当前页
   * @param pageSize       每页显示条数
   * @param startTime      开始时间
   * @param endTime        结束时间
   * @param fields         需要显示的字段，逗号分隔（缺省为全部字段）
   * @param sortField      排序字段
   * @param matchPhrase    true 使用，短语精准匹配
   * @param highlightField 高亮字段
   * @param matchStr       过滤条件（xxx=111,aaa=222）
   * @return
   * @throws IOException 
   */
  public static EsPagnationData searchDataPage(String index, String type, int currentPage, int pageSize, long startTime, long endTime, String fields, String sortField, boolean matchPhrase, String highlightField, String matchStr) throws IOException {
      
  	SearchRequest searchRequest = new SearchRequest(index.split(","));
  	SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
  	
      if (StringUtils.isNotEmpty(type)) {
      	searchRequest.types(type.split(","));
      }
      // 需要显示的字段，逗号分隔（缺省为全部字段）
      if (StringUtils.isNotEmpty(fields)) {
      	searchSourceBuilder.fetchSource(fields.split(","), null);
      }

      //排序字段
      if (StringUtils.isNotEmpty(sortField)) {
      	searchSourceBuilder.sort(new FieldSortBuilder(sortField).order(SortOrder.ASC)); 
      }

      BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

      if (startTime > 0 && endTime > 0) {
          boolQuery.must(QueryBuilders.rangeQuery("processTime")
                  .format("epoch_millis")
                  .from(startTime)
                  .to(endTime)
                  .includeLower(true)
                  .includeUpper(true));
      }

      // 查询字段
      if (StringUtils.isNotEmpty(matchStr)) {
          for (String s : matchStr.split(",")) {
              String[] ss = s.split("=");
              if (matchPhrase == Boolean.TRUE) {
                  boolQuery.must(QueryBuilders.matchPhraseQuery(s.split("=")[0], s.split("=")[1]));
              } else {
                  boolQuery.must(QueryBuilders.matchQuery(s.split("=")[0], s.split("=")[1]));
              }
          }
      }

      // 高亮（xxx=111,aaa=222）
      if (StringUtils.isNotEmpty(highlightField)) {
          HighlightBuilder highlightBuilder = new HighlightBuilder();

          //highlightBuilder.preTags("<span style='color:red' >");//设置前缀
          //highlightBuilder.postTags("</span>");//设置后缀

          // 设置高亮字段
          highlightBuilder.field(highlightField);
          searchSourceBuilder.highlighter(highlightBuilder);
      }

      searchSourceBuilder.query(QueryBuilders.matchAllQuery());
      searchSourceBuilder.query(boolQuery);

      // 分页应用
      searchSourceBuilder.from(currentPage).size(pageSize);

      // 设置是否按查询匹配度排序
      searchSourceBuilder.explain(true);

      //打印的内容 可以在 Elasticsearch head 和 Kibana  上执行查询
      LOGGER.info("\n{}", searchSourceBuilder);

      // 执行搜索,返回搜索响应信息
      SearchResponse searchResponse = client.search(searchRequest);

      long totalHits = searchResponse.getHits().totalHits;
      long length = searchResponse.getHits().getHits().length;

      LOGGER.debug("共查询到[{}]条数据,处理数据条数[{}]", totalHits, length);

      if (searchResponse.status().getStatus() == 200) {
          // 解析对象
          List<Map<String, Object>> sourceList = setSearchResponse(searchResponse, highlightField);

          return new EsPagnationData(currentPage, pageSize, (int) totalHits, sourceList);
      }

      return null;

  }

  
  
}
