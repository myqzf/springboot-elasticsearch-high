package com.elastic.service;

import java.util.List;

import com.elastic.model.Book;

public interface BookService {

  Book save(Book book);

  void delete(Book book);

  Iterable<Book> findAll();
  
	Book findById(Long id);
	
	List<Book> searchBook(String searchField, String searchContent);

	void deleteById(Long id);


}
