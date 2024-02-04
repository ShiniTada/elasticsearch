package com.epam.elasticsearch.controller;

import com.epam.elasticsearch.dto.CustomError;
import org.apache.http.StatusLine;
import org.elasticsearch.client.ResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class GlobalExceptionHandler {

    private final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(value = ResponseException.class)
    public ResponseEntity<Object> handleAllExceptions(ResponseException e) {
        StatusLine statusLine = e.getResponse().getStatusLine();
        logger.warn("Warn: ", statusLine);
        HttpStatus statusCode = HttpStatus.valueOf(statusLine.getStatusCode());
        CustomError error = new CustomError(statusCode, statusLine.getReasonPhrase());
        return new ResponseEntity<>(error, statusCode);
    }

    /**
     * this will handle all types of exceptions
     */
    @ExceptionHandler(value = Exception.class)
    public ResponseEntity<Object> handleAllExceptions(Exception e) {
        logger.error("Error: ", e);
        CustomError error = new CustomError(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage());
        return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}