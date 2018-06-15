//package com.elastic.service.impl;
//
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//
//import com.elastic.model.Content;
//import com.elastic.repository.ContentRepository;
//import com.elastic.service.ElasticsearchService;
//
//@Service
//public class ElasticsearchServiceImpl implements ElasticsearchService {
//
//	@Autowired
//	private ContentRepository contentRepository;
//	@Override
//	public void save(Content content) {
//		System.out.println(content.getCode());
//		contentRepository.save(content);
//	}
//
//}
