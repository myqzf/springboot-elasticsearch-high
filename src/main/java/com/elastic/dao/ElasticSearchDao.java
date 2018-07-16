package com.elastic.dao;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;
import com.elastic.utils.EsPagnationData;

public interface ElasticSearchDao {

	boolean createIndexSync(String index);

	boolean createIndexASync(String index);

	boolean isIndexExist(String index);

	boolean deleteIndex(String index);
	
	boolean putMapping(JSONObject jsonObject, String index,String type);

	boolean isDocumentExist(String index, String type, String id);

	String addDocumnetForId(JSONObject jsonObject, String index, String type, String id);

	String addDocumnet(JSONObject jsonObject, String index, String type);

	void deleteDocumentByIdSync(String index, String type, String id) throws IOException;

	void deleteDocumentByIdAsync(String index, String type, String id);

	void updateDataByIdSync(JSONObject jsonObject, String index, String type, String id) throws IOException;

	void updateDataByIdASync(JSONObject jsonObject, String index, String type, String id);

	String getDocumentById(String index, String type, String id, String includFields, String excludFields) throws IOException;

	List<Map<String, Object>> searchListData(String index, String type, long startTime, long endTime, Integer size, String fields, String sortField, boolean matchPhrase, String highlightField, String matchStr) throws IOException;

	EsPagnationData searchDataPage(String index, String type, int currentPage, int pageSize, long startTime, long endTime, String fields, String sortField, boolean matchPhrase, String highlightField, String matchStr) throws IOException;

	<E> Map<String, Object> ScrollSearch(String index, Class<E> ec, String referScoreId, String searchContent, String includeFields, String excludeFields);

}
