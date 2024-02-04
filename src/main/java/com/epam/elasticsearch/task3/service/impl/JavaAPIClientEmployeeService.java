package com.epam.elasticsearch.task3.service.impl;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.AvgAggregate;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsAggregate;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.CreateResponse;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.epam.elasticsearch.task3.dto.Employee;
import com.epam.elasticsearch.task3.service.EmployeeService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.RestClient;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service("JavaAPIClientEmployeeService")
@Profile("task4")
public class JavaAPIClientEmployeeService implements EmployeeService {

    private final ElasticsearchClient esClient;
    private final ObjectMapper objectMapper;

    public JavaAPIClientEmployeeService(RestClient restClient, ObjectMapper objectMapper) {
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        this.esClient = new ElasticsearchClient(transport);
        this.objectMapper = objectMapper;
    }

    @Override
    public Employee getById(String id) throws IOException {
        GetResponse<Map> response = esClient.get(g -> g.index("employees").id(id), Map.class);
        Map<String, Object> sourceMap = response.source();

        Employee employee = objectMapper.convertValue(sourceMap, Employee.class);
        employee.setId(id);
        return employee;
    }

    @Override
    public List<Employee> getAll() throws IOException {
        SearchResponse<Map> response = esClient.search(s -> s
                        .index("employees")
                        .query(q -> q.matchAll(QueryBuilders.matchAll().build())),
                        Map.class);
        return parseListResponse(response);
    }

    @Override
    public Employee create(String id, Employee employee) throws IOException {
        Map<String, Object> employeeMap = objectMapper.convertValue(employee, Map.class);
        CreateResponse response = esClient.create(req -> req
                        .index("employees")
                        .id(id)
                        .document(employeeMap));
        employee.setId(response.id());
        return employee;
    }

    @Override
    public void delete(String id) throws IOException {
        DeleteResponse response = esClient.delete(d -> d.index("employees").id(id));
        if (response.result().equals(Result.NotFound)) {
            throw new RuntimeException("Document is not found. Id: " + id);
        }
    }

    @Override
    public List<Employee> searchByField(String fieldName, String fieldValue) throws IOException {
        SearchResponse<Map> response = esClient.search(s -> s
                        .index("employees")
                        .query(q -> q.match(t -> t.field(fieldName).query(fieldValue))),
                        Map.class);
        return parseListResponse(response);
    }

    @Override
    public Map<String, Object> aggregate(String aggregationField, String metricType, String metricField) throws IOException {
        String aggName = "metric_agg";
        String subAggName = "metric";
        SearchResponse<Map> response = esClient.search(s -> s
                .size(0)
                .index("employees")
                .aggregations(aggName, a -> a
                        .terms(term -> term.field(aggregationField))
                        .aggregations(subAggName, a2 -> a2._custom(metricType, new MetricField(metricField)))
                ),
                Map.class);
        StringTermsAggregate aggregation = (StringTermsAggregate) response.aggregations().get(aggName)._get();
        List<CustomBucket> customBuckets = new ArrayList<>();
        aggregation.buckets().array().forEach(bucket -> {
            AvgAggregate subAggregation = (AvgAggregate) bucket.aggregations().get(subAggName)._get();
            var customMetric = new CustomMetric(subAggregation.value());
            var customBucket = new CustomBucket(bucket.key()._get().toString(), bucket.docCount(), customMetric);
            customBuckets.add(customBucket);
        });
        return Map.of(aggName, new CustomAggregation(aggregation.docCountErrorUpperBound(), aggregation.sumOtherDocCount(), customBuckets));
    }

    private List<Employee> parseListResponse(SearchResponse<Map> response) {
        List<Employee> employees = new ArrayList<>();
        for (Hit<Map> hit : response.hits().hits()) {
            Map<String, Object> sourceMap = hit.source();
            Employee employee = objectMapper.convertValue(sourceMap, Employee.class);
            employee.setId(hit.id());
            employees.add(employee);
        }
        return employees;
    }

    private record MetricField(String field) {}

    private record CustomAggregation(long docCountErrorUpperBound, long sumOtherDocCount, List<CustomBucket> buckets) {}

    private record CustomBucket(String key, long doc_count, CustomMetric metric) {}

    private record CustomMetric(double value) {}
}
