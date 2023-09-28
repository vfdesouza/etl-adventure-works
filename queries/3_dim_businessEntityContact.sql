CREATE TABLE marts.dim_businessEntityContact (
    BusinessEntityID varchar(255) PRIMARY KEY,
    PersonID varchar(255),
    ContactTypeID varchar(255) REFERENCES dim_contactType(ContactTypeID),
    ModifiedData date
);
