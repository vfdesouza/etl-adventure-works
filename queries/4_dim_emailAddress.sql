CREATE TABLE marts.dim_emailAddress (
    BusinessEntityID varchar(255) REFERENCES dim_businessEntityContact(BusinessEntityID),
    EmailAddressID varchar(255) PRIMARY KEY,
    EmailAddress varchar(255)
);
