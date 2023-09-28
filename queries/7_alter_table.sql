ALTER TABLE marts.dim_businessEntityContact
ADD CONSTRAINT fk_dim_businessentitycontact_person
FOREIGN KEY (PersonID)
REFERENCES marts.fact_person(BusinessEntityID);
