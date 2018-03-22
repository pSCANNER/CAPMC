/***********************************************************

--	DRC PII Table DDL

***********************************************************/
--DRC PII Name table
create table omop5.DRC_PII_NAME (
	PERSON_ID BIGINT,
	FIRST_NAME VARCHAR(50),
	MIDDLE_NAME VARCHAR(50),
	LAST_NAME VARCHAR(50),
	SUFFIX VARCHAR(50),
	PREFIX VARCHAR(50)
)
	
--DRC PII Email table
create table omop5.DRC_PII_EMAIL (
	PERSON_ID BIGINT,
	EMAIL VARCHAR(100)
)	


--DRC PII Phone Number table
create table omop5.DRC_PII_PHONE_NUMBER(
	PERSON_ID BIGINT,
	PHONE_NUMBER VARCHAR(50)
)	

--DRC PII Address table
create table omop5.DRC_PII_ADDRESS(
	PERSON_ID BIGINT,
	LOCATION_ID BIGINT
)	


--DRC PII MRN table
create table omop5.DRC_PII_MRN (
	PERSON_ID BIGINT,
	HEALTH_SYSTEM VARCHAR(50),
	MRN VARCHAR(50)
)


/***********************************************************

--	ETL from CAPMC identity tables to DRC PII Tables 

***********************************************************/

--DRC PII Name table
insert into omop5.DRC_PII_NAME (
	PERSON_ID,
	FIRST_NAME ,
	MIDDLE_NAME ,
	LAST_NAME ,
	SUFFIX ,
	PREFIX 
)
select right(iden.identifier, (LEN(iden.identifier)-1)) as [PERSON_ID] --Acc to DRC: This is the PMI_ID (with the letter removed)
	, person.first_name  as [FIRST_NAME]
	, person.middle_name as [MIDDLE_NAME]
	, person.surname as [LAST_NAME]
	, coalesce(person.qualifications, person.honorifics) as [SUFFIX]
	, person.title as [PREFIX]
from omop5.phi_person person
join omop5.phi_identifier iden on iden.master_id = person.master_id 
	and iden.id_type_concept_id = 2000000813 -- PMI_ID



	
--DRC PII Email table
insert into omop5.DRC_PII_EMAIL (
	PERSON_ID ,
	EMAIL 
)	
select right(iden.identifier, (LEN(iden.identifier)-1)) as [PERSON_ID] --Acc to DRC: This is the PMI_ID (with the letter removed)
	, email.value  as [EMAIL]
from omop5.phi_telecom email
join omop5.phi_identifier iden on iden.master_id = email.master_id 
	and iden.id_type_concept_id = 2000000813 -- PMI_ID
where email.use_concept_id = 2000000405 -- Email





--DRC PII Phone Number table
insert into omop5.DRC_PII_PHONE_NUMBER(
	PERSON_ID,
	PHONE_NUMBER 
)	
select right(iden.identifier, (LEN(iden.identifier)-1)) as [PERSON_ID] --Acc to DRC: This is the PMI_ID (with the letter removed)
	, Phone.value  as [PHONE_NUMBER]
from omop5.phi_telecom Phone
join omop5.phi_identifier iden on iden.master_id = Phone.master_id 
	and iden.id_type_concept_id = 2000000813 -- PMI_ID
where Phone.use_concept_id in ( 		2000000400		-- Home phone		,2000000401		-- Work phone
		) 





----DRC PII Address table
declare @OMOP_PERSON_ID varchar(50) = 'OMOP_PERSON_ID'
declare @PMI_ID varchar(50) = 'PMI_ID'

insert into omop5.DRC_PII_ADDRESS(
	PERSON_ID ,
	LOCATION_ID 
)	

select right(iden.identifier, (LEN(iden.identifier)-1))  as [PERSON_ID] --Acc to DRC: This is the PMI_ID (with the letter removed)
	, loc.location_id as [LOCATION_ID]
from omop5.location loc 
join omop5.phi_identifier person on on loc.person_id = person.identifier  
	and person.id_type_concept_id = @OMOP_PERSON_ID -- OMOP_Person_id

join omop5.phi_identifier iden on on iden.master_id = person.master_id 
	and iden.id_type_concept_id = @PMI_ID -- PMI_ID




--DRC PII MRN table
insert into omop5.DRC_PII_MRN (
	PERSON_ID ,
	HEALTH_SYSTEM ,
	MRN 
)
select right(iden.identifier, (LEN(iden.identifier)-1)) as [PERSON_ID] --Acc to DRC: This is the PMI_ID (with the letter removed)
	, person.source_system as [HEALTH_SYSTEM]
	, person.mrn  as [MRN]
from omop5.phi_person person
join omop5.phi_identifier iden on iden.master_id = person.master_id 
	and iden.id_type_concept_id = 2000000813 -- PMI_ID
