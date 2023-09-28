CREATE TABLE marts.dim_personPhone (
    BusinessEntityID varchar(255) REFERENCES dim_businessEntityContact(BusinessEntityID),
    PhoneNumber varchar(255) PRIMARY KEY,
    PhoneNumberTypeID varchar(255) REFERENCES dim_phoneNumberType(PhoneNumberTypeID)
);
