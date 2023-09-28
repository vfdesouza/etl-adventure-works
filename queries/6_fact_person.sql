CREATE TABLE marts.fact_person (
    BusinessEntityID varchar(255) PRIMARY KEY REFERENCES dim_businessEntityContact(BusinessEntityID),
    PersonType varchar(255),
    NameStyle varchar(255),
    Title varchar(255),
    FirstName varchar(255),
    MiddleName varchar(255),
    LastName varchar(255),
    Suffix varchar(255),
    EmailPromotion varchar(255),
    AdditionalContactInfo varchar(255),
    Demographics varchar(255),
    ModifiedDate date
);
