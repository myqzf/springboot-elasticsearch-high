package com.elastic.controller;

import java.util.List;

import javax.websocket.server.PathParam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.elastic.model.Book;
import com.elastic.service.impl.BookServiceImpl;

@RestController
public class BookController {

    private final static Logger logger = LoggerFactory.getLogger(BookController.class);

    @Autowired
    private BookServiceImpl bookService;

    /**
     * 保存book
     * @return
     */
    @RequestMapping(value = "/saveBook", method = RequestMethod.POST)
  	@ResponseBody
    public Book saveBook(@RequestBody Book book) {
        book.setReleaseDate("添加一本书。");
        return bookService.save(book);
    }
    
    /**
     * 根据id获取book
     * @return
     */
    @RequestMapping(value = "/getBook/{id}", method = RequestMethod.GET)
  	@ResponseBody
    public Book getBook(@PathVariable Long id) {
    	return bookService.findById(id);
    }
    
    /**
     * 根据id获取book
     * @return
     */
    @RequestMapping(value = "/deleteBook/{id}", method = RequestMethod.GET)
  	@ResponseBody
    public void deleteBook(@PathVariable Long id) {
    	bookService.deleteById(id);
    }
    /**
     * 根据id获取book
     * @return
     */
    @RequestMapping(value = "/searchBooks", method = RequestMethod.GET)
  	@ResponseBody
    public List<Book> searchBooks(@RequestParam String searchField, @RequestParam String searchVlaue) {
    	return bookService.searchBook(searchField, searchVlaue);
    }
    
    /**
     * 根据id获取book
     * @return
     */
    @RequestMapping(value = "/getBooks", method = RequestMethod.GET)
  	@ResponseBody
    public List<Book> getBooks() {
    	return  bookService.findAll();
    }

   
}
