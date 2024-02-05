package com.epam.elasticsearch.dto;

import lombok.Data;

import java.time.LocalDate;
import java.util.List;

@Data
public class Employee {
    private String id;
    private String name;
    private LocalDate dob;
    private Address address;
    private String email;
    private List<String> skills;
    private int experience;
    private double rating;
    private String description;
    private boolean verified;
    private long salary;
}
