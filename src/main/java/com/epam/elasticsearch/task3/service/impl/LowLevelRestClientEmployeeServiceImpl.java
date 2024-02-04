package com.epam.elasticsearch.task3.service.impl;

import com.epam.elasticsearch.task3.dto.Employee;
import com.epam.elasticsearch.task3.service.EmployeeService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class LowLevelRestClientEmployeeServiceImpl implements EmployeeService {

    private final RestClient client;
    private final ObjectMapper objectMapper;

    public LowLevelRestClientEmployeeServiceImpl(RestClient client, ObjectMapper objectMapper) {
        this.client = client;
        this.objectMapper = objectMapper;
    }

    @Override
    public Employee getById(String id) throws IOException {
        Request request = new Request("GET", "/employees/_doc/" + id);
        Response response = client.performRequest(request);

        JsonNode jsonNode = objectMapper.readTree(EntityUtils.toString(response.getEntity()));
        Employee employee = objectMapper.treeToValue(jsonNode.get("_source"), Employee.class);
        employee.setId(id);
        return employee;
    }

    @Override
    public List<Employee> getAll() throws IOException {
        Request request = new Request("GET", "/employees/_search");
        Response response = client.performRequest(request);
        return parseListResponse(response);
    }

    @Override
    public Employee create(String id, Employee employee) throws IOException {
        Request request = new Request("POST", "/employees/_doc/"+ id);
        String jsonString = objectMapper.writeValueAsString(employee);
        request.setJsonEntity(jsonString);
        Response response = client.performRequest(request);

        JsonNode jsonNode = objectMapper.readTree(EntityUtils.toString(response.getEntity()));
        if (jsonNode.path("result").asText().equals("created")) {
            employee.setId(jsonNode.path("_id").asText());
            return employee;
        } else {
            throw new RuntimeException("Failed to create/replace the document.");
        }
    }

    @Override
    public void delete(String id) throws IOException {
        Request request = new Request("DELETE", "/employees/_doc/" + id);
        Response response = client.performRequest(request);
        // Check response status, throw an exception if e.g. 404 not found
        if (response.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND) {
            throw new ResponseException(response);
        }
    }

    @Override
    public List<Employee> searchByField(String fieldName, String fieldValue) throws IOException {
        String queryJson = """
                {
                    "query": {
                        "match" : {
                            "%s" : "%s"
                        }
                    }
                }
                """.formatted(fieldName, fieldValue);
        Request request = new Request("GET", "/employees/_search");
        request.setJsonEntity(queryJson);
        Response response = client.performRequest(request);

        return parseListResponse(response);
    }

    @Override
    public Map<String, Object> aggregate(String aggregationField, String metricType, String metricField) throws IOException {
        String queryJson = """
                {
                    "size": 0,
                    "aggs": {
                        "metric_agg": {
                            "terms": {
                                "field": "%s"
                            },
                            "aggs": {
                                "metric": {
                                    "%s": {
                                        "field": "%s"
                                    }
                                }
                            }
                        }
                    }
                }
                """.formatted(aggregationField, metricType, metricField);
        Request request = new Request("GET", "/employees/_search");
        request.setJsonEntity(queryJson);

        Response response = client.performRequest(request);
        JsonNode jsonNode = objectMapper.readTree(EntityUtils.toString(response.getEntity()));

        return objectMapper.treeToValue(jsonNode.get("aggregations"), Map.class);
    }

    private List<Employee> parseListResponse(Response response) throws IOException {
        List<Employee> employees = new ArrayList<>();
        JsonNode jsonNode = objectMapper.readTree(EntityUtils.toString(response.getEntity()));
        jsonNode.get("hits").get("hits").forEach(hitNode -> {
            try {
                Employee employee = objectMapper.treeToValue(hitNode.get("_source"), Employee.class);
                employee.setId(objectMapper.treeToValue(hitNode.get("_id"), String.class));
                employees.add(employee);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
        return employees;
    }
}
