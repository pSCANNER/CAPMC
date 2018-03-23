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

declare @OMOP_PERSON_CONCEPT_TYPE_ID bigint = 2000000812	--OMOP_PERSON_ID 
declare @PMI_ID_TYPE_CONCEPT_ID bigint = 2000000813 -- PMI_ID


Declare @HOMEPHONE_CONCEPT_TYPE_ID bigint = 2000000400 -- Home phone
Declare @WORKPHONE_CONCEPT_TYPE_ID bigint = 2000000401 -- Work phone
DECLARE @EMAIL_CONCEPT_TYPE_ID BIGINT =  2000000405 -- Email


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
	and iden.id_type_concept_id = @PMI_ID_TYPE_CONCEPT_ID -- PMI_ID
group by iden.identifier, person.first_name, person.middle_name, person.surname, person.qualifications, person.honorifics, person.title


	
	----------
	
--DRC PII Email table
insert into omop5.DRC_PII_EMAIL (
	PERSON_ID ,
	EMAIL 
)	
select right(iden.identifier, (LEN(iden.identifier)-1)) as [PERSON_ID] --Acc to DRC: This is the PMI_ID (with the letter removed)
	, email.value  as [EMAIL]
from omop5.phi_telecom email
join omop5.phi_identifier iden on iden.master_id = email.master_id 
	and iden.id_type_concept_id = @PMI_ID_TYPE_CONCEPT_ID
where email.use_concept_id = @EMAIL_CONCEPT_TYPE_ID
group by iden.identifier, email.value




--DRC PII Phone Number table
insert into omop5.DRC_PII_PHONE_NUMBER(
	PERSON_ID,
	PHONE_NUMBER 
)	
select right(iden.identifier, (LEN(iden.identifier)-1)) as [PERSON_ID] --Acc to DRC: This is the PMI_ID (with the letter removed)
	, Phone.value  as [PHONE_NUMBER]
from omop5.phi_telecom Phone
join omop5.phi_identifier iden on iden.master_id = Phone.master_id 
	and iden.id_type_concept_id = @PMI_ID_TYPE_CONCEPT_ID
where Phone.use_concept_id in ( 		@HOMEPHONE_CONCEPT_TYPE_ID				,@WORKPHONE_CONCEPT_TYPE_ID		-- Work phone
		) 
group by iden.identifier, phone.value






----DRC PII Address table
insert into omop5.DRC_PII_ADDRESS(
	PERSON_ID ,
	LOCATION_ID 
)	
select right(iden.identifier, (LEN(iden.identifier)-1))  as [PERSON_ID] --Acc to DRC: This is the PMI_ID (with the letter removed)
	, per.location_id as [LOCATION_ID]
from omop5.person per 
join omop5.phi_identifier OMOP_Person_id on per.person_id = OMOP_Person_id.identifier  
	and OMOP_Person_id.id_type_concept_id = @OMOP_PERSON_CONCEPT_TYPE_ID -- OMOP_Person_id
join omop5.phi_identifier iden on iden.master_id = OMOP_Person_id.master_id 
	and iden.id_type_concept_id = @PMI_ID_TYPE_CONCEPT_ID -- PMI_ID
group by iden.identifier, per.location_id



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
	and iden.id_type_concept_id = @PMI_ID_TYPE_CONCEPT_ID 
group by iden.identifier, person.source_system, person.mrn