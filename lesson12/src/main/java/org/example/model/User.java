package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import java.util.Date;


@Value
@AllArgsConstructor
@Builder(toBuilder = true)
public class User {
    String id;
    String firstName;
    String lastName;
    int age;
}
