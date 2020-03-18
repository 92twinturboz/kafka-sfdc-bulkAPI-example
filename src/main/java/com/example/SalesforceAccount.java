package com.example;

public class SalesforceAccount {

    String name;
    String description;
    String accountNumber;

    public SalesforceAccount() {

    }

    public SalesforceAccount(String name, String description, String accountNumber) {
        this.name = name;
        this.description = description;
        this.accountNumber = accountNumber;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getAccountNumber() {
        return accountNumber;
    }

    public void setAccountNumber(String accountNumber) {
        this.accountNumber = accountNumber;
    }

    public String toString() {
        return new com.google.gson.Gson().toJson(this);
    }
}
