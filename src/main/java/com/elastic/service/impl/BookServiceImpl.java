package com.elastic.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.store.Directory;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.elastic.dao.impl.ElasticSearchDaoImpl;
import com.elastic.model.Book;
import com.elastic.service.BookService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class BookServiceImpl implements BookService {
	private static final Logger LOGGER = LoggerFactory.getLogger(BookServiceImpl.class);
	@Autowired
	private RestHighLevelClient restHighLevelClient;
	@Autowired
	private ElasticSearchDaoImpl elasticSearchDao;
	@Autowired
	private ObjectMapper jsonObjectMapper;
	@Autowired
	@Qualifier("cmsElasticSearchIndex")
	private String cmsIndex;

	@Override
	public Book save(Book book) {
		JSONObject json = (JSONObject) JSON.toJSON(book);
		elasticSearchDao.addDocumnetForId(json, cmsIndex, Book.class.getSimpleName(), book.getId().toString());
		return book;
	}

	@Override
	public Book findById(Long id) {
		Book book = null;
		try {
			String result =  elasticSearchDao.getDocumentById(cmsIndex, Book.class.getSimpleName(), id.toString(), "id,title,author", null);
			book = JSONObject.parseObject(result, Book.class);
			System.out.println(result);
		} catch (IOException e) {
			LOGGER.error("根据Id获取文档数据失败！");
			e.printStackTrace();
		}
		return book;
	}

	@Override
	public void deleteById(Long id) {
		elasticSearchDao.deleteDocumentByIdAsync(cmsIndex, Book.class.getSimpleName(), id.toString());
	}

	@Override
	public List<Book> findAll() {
		List<Book> list = new ArrayList<>();
		
//		SearchRequest searchRequest = new SearchRequest();
//		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
//		searchSourceBuilder.query(QueryBuilders.matchAllQuery()); //向SearchSourceBuilder添加一个查询match_all查询
//		searchRequest.source(searchSourceBuilder);
//		try {
//			SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
//			SearchHits hits = searchResponse.getHits();
//			SearchHit[] searchHits = hits.getHits();
//			for (SearchHit hit : searchHits) {
//				String result = hit.getSourceAsString();
//				list.add(jsonObjectMapper.readValue(result, new TypeReference<Book>() {
//				}));
//			}
//		} catch (IOException e) {
//			LOGGER.error("获取全部文档数据失败！");
//			e.printStackTrace();
//		}
		return list;
	}

	@Override
	public List<Book> searchBook(String searchField, String searchVlaue) {
		
		
		return null;
	}

	@Override
	public void delete(Book book) {
		deleteById(book.getId());
	}

	
}
