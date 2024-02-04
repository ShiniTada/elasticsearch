package com.epam.elasticsearch.service;


import com.epam.elasticsearch.dto.Employee;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface EmployeeService {
    Employee getById(String id) throws IOException;

    List<Employee> getAll() throws IOException;

    Employee create(String id, Employee employee) throws IOException;

    void delete(String id) throws IOException;

    List<Employee> searchByField(String fieldName, String fieldValue) throws IOException;

    Map<String, Object> aggregate(String aggregationField, String metricType, String metricField) throws IOException;
}
