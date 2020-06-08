/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.batchdatagenerator;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
/**
 * This class serves as a copy of of io.codearte.jfairy.producer.person.Person
 * because io.codearte.jfairy.producer.person.Person does not
 * have default constructors needed to deserialize POJOs
 */
public class Person {
    private Address address;
    private String firstName;
    private String middleName;
    private String lastName;
    private String email;
    private String username;
    private String password;
    private Sex sex;
    private String telephoneNumber;
    @org.apache.avro.reflect.AvroSchema("{ \"type\": \"long\", \"logicalType\": \"timestamp-millis\" }")
    private long dateOfBirth;
    private Integer age;
    private Company company;
    private String companyEmail;
    private String nationalIdentityCardNumber;
    private String nationalIdentificationNumber;
    private String passportNumber;

    public enum Sex {
        MALE,
        FEMALE;

        private Sex() {
        }
    }

    public Person(io.codearte.jfairy.producer.person.Person person) {
        this(new Address(person.getAddress()),
             person.getFirstName(),
             person.getMiddleName(),
             person.getLastName(),
             person.getEmail(),
             person.getUsername(),
             person.getPassword(),
             Sex.valueOf(person.getSex().name()),
             person.getTelephoneNumber(),
             person.getDateOfBirth().getMillis(),
             person.getAge(),
             new Company(person.getCompany()),
             person.getCompanyEmail(),
             person.getNationalIdentityCardNumber(),
             person.getNationalIdentificationNumber(),
             person.getPassportNumber());
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Company {
        private String name;
        private String domain;
        private String email;
        private String vatIdentificationNumber;
        public Company(io.codearte.jfairy.producer.company.Company company) {
            this(company.getName(),
                 company.getDomain(),
                 company.getEmail(),
                 company.getVatIdentificationNumber());
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Address {
        protected String street;
        protected String streetNumber;
        protected String apartmentNumber;
        protected String postalCode;
        protected String city;

        public Address(io.codearte.jfairy.producer.person.Address address) {
            this(address.getStreet(),
                 address.getStreetNumber(),
                 address.getApartmentNumber(),
                 address.getPostalCode(),
                 address.getCity());
        }
    }
}
