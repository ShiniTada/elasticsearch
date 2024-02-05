package com.epam.elasticsearch.controller;

import com.epam.elasticsearch.dto.Employee;
import com.epam.elasticsearch.service.EmployeeService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@Profile("task3")
@RequestMapping(value = "/low-level-rest-client/employees")
@Tag(name = "Task 3. Java Low Level REST Client")
public class LowLevelRestClientEmployeeController {

    private final EmployeeService service;

    public LowLevelRestClientEmployeeController(@Qualifier("LowLevelRestClientEmployeeService") EmployeeService service) {
        this.service = service;
    }

    @GetMapping("/{id}")
    @Operation(summary = "Get an employee by id")
    public Employee getEmployeeById(@PathVariable String id) throws IOException {
        return service.getById(id);
    }

    @GetMapping
    @Operation(summary = "Get all employees")
    public List<Employee> getAllEmployees() throws IOException {
        return service.getAll();
    }

    @PostMapping("/{id}")
    @Operation(summary = "Create an employee providing id and employee data json")
    public Employee createEmployee(@PathVariable String id, @RequestBody Employee employee) throws IOException {
        return service.create(id, employee);
    }

    @DeleteMapping("/{id}")
    @Operation(summary = "Delete an employee by its id")
    public void deleteEmployee(@PathVariable String id) throws IOException {
        service.delete(id);
    }

    @GetMapping("/search")
    @Operation(summary = "Search employees by any field")
    public List<Employee> searchEmployee(@RequestParam String field, @RequestParam String value) throws IOException{
        return service.searchByField(field, value);
    }

    @GetMapping("/aggregate")
    @Operation(summary = "Perform an aggregation by any numeric field with metric calculation")
    public Map<String, Object> aggregate(@RequestParam String aggregationField, @RequestParam String metricType, @RequestParam String metricField) throws IOException{
        return service.aggregate(aggregationField, metricType, metricField);
    }
}
