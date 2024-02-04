package com.epam.elasticsearch.task3.dto;

import org.springframework.http.HttpStatus;

public class CustomError {

    private final HttpStatus status;
    private final String message;

    public CustomError(HttpStatus status, String message) {
        this.status = status;
        this.message = message;
    }

    public HttpStatus getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }
}
