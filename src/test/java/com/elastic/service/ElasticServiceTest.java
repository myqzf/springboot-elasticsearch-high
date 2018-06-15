package com.elastic.service;

import java.io.IOException;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.elastic.model.Book;
import com.elastic.service.impl.BookServiceImpl;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ElasticServiceTest {
	@Autowired
	private BookServiceImpl bookService;
	@Autowired
	private RestHighLevelClient client;

	@Test
	public void getIndexTest() {
		GetRequest request = new GetRequest("cms", "Book", "1");
		try {
			GetResponse response = client.get(request);
			System.out.println(response);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void getIndexServiceTest() {
		Book book = bookService.findById(1L);
		System.out.println(book.toString());
	}

}