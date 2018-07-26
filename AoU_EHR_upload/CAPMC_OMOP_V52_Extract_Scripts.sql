/*****************************************************************************
-- Author: Paulina Paul
-- Create date: 03/21/2018
-- Description: Extract initial 6 OMOP v5 tables to mimic OMOP v5.2 CDM 
--				for CAPMC EHR data submission
-- Tables:	
--		Person
--		Visit_occurrence
--		Condition_Occurrence
--		Procedure_Occurrence
--		Measurement
--		Drug_Exposure 
*****************************************************************************/
truncate table omop_v5_pcornet_v3.CAPMC_UCSD.DRC_PII_ADDRESS 
truncate table omop_v5_pcornet_v3.CAPMC_UCSD.DRC_PII_EMAIL 
truncate table omop_v5_pcornet_v3.CAPMC_UCSD.DRC_PII_MRN
truncate table omop_v5_pcornet_v3.CAPMC_UCSD.DRC_PII_NAME 
truncate table omop_v5_pcornet_v3.CAPMC_UCSD.DRC_PII_PHONE_NUMBER



--HealthPro patients
if OBJECT_ID ('tempdb.dbo.#PMI') is not null drop table #PMI
select [PMI ID], --cast('' as varchar(50)) as master_id, 0 as person_id,
PMI_ID.master_id, omop_id.identifier as person_id,
[General Consent Status], [General Consent Date], 
[EHR Consent Status], [EHR Consent Date],
[CABoR Consent Status], [CABoR Consent Date],
[Withdrawal Status], [Withdrawal Date] , [Paired Organization]
into #PMI
from PMI.HealthPro.workqueue w 
left join omop5.phi_identifier PMI_ID on PMI_ID.identifier = w.[pmi id] 
				and PMI_ID.id_type_concept_id = 2000000813 --PMI ID       
left join omop5.phi_identifier omop_id on omop_id.master_id = PMI_ID.master_id 
				and omop_id.id_type_concept_id = 2000000812 --OMOP person ID
where [Paired Organization] = 'CAL_PMC_UCSD'
and [General Consent Status] = 1
and [EHR Consent Status] = 1
and [Withdrawal Status] != 1
and [CABoR Consent Date] !=''



--update pmi
--set pmi.master_id = PMI_ID.master_id
--, pmi.person_id = omop_id.identifier 
--from #pmi pmi 
--join omop5.phi_identifier PMI_ID on PMI_ID.identifier = pmi.[pmi id] 
--				and PMI_ID.id_type_concept_id = 2000000813 --PMI ID       
--left join omop5.phi_identifier omop_id on omop_id.master_id = PMI_ID.master_id 
--				and omop_id.id_type_concept_id = 2000000812 --OMOP person ID
 
 
 
-------------------------------------------------------------------------------------
		--testing

		select * from #pmi WHERE MASTER_ID IS NOT NULL 

	
select [PMI id], COUNT(*) from #PMI group by [PMI id] having count(*) >1
select master_id, COUNT(*) from #PMI group by master_id having count(*) >1
select person_id, COUNT(*) from #PMI group by person_id having count(*) >1
 
		


	select pmi.*, PMI_ID.* 
	 from #pmi pmi 
	left join omop5.phi_identifier PMI_ID on PMI_ID.identifier = pmi.[pmi id] 
				and PMI_ID.id_type_concept_id = 2000000813 --PMI ID    
	 where pmi.master_id = ''


select * from omop5.phi_identifier PMI_ID --on PMI_ID.identifier = pmi.[pmi id] 
				where --PMI_ID.id_type_concept_id = 2000000813 --PMI ID  
				--and 
				identifier like 'p%'


-------------------------------------------------------------------------------------

--Person table
if OBJECT_ID('OMOP_V5_Pcornet_V3.CAPMC_UCSD.person') is not null drop table OMOP_V5_Pcornet_V3.CAPMC_UCSD.person
select distinct  cast(replace(PMI.[pmi id], 'P', '') as bigint) as [person_id]  --per DRC, Person_id = PMI_ID (remove 'P')
      , gc.target_concept_id as   [gender_concept_id]
      ,[year_of_birth]
      ,[month_of_birth]
      ,[day_of_birth]
      ,convert(nvarchar(30), cast(DATEFROMPARTS ( [year_of_birth], [month_of_birth], [day_of_birth] ) as datetime), 126)    birth_datetime
      ,rc.target_concept_id as [race_concept_id]
      ,ec.target_concept_id as [ethnicity_concept_id]
      ,[location_id]
      ,[provider_id]
      ,[care_site_id]
      ,[person_source_value]
      ,[gender_source_value]
      ,[gender_source_concept_id]
      ,[race_source_value]
      ,[race_source_concept_id]
      ,[ethnicity_source_value]
      ,[ethnicity_source_concept_id]
     into OMOP_V5_Pcornet_V3.CAPMC_UCSD.person 
       from omop5.person person
       join #pmi pmi on pmi.person_id = person.person_id 
       left join [OMOP_VOCABULARY].[AoU_DRC].[local_source_to_concept_map] gc on gc.source_concept_id = person.gender_concept_id
		left join   [OMOP_VOCABULARY].[AoU_DRC].[local_source_to_concept_map] rc on rc.source_concept_id = person.race_concept_id     
		left join   [OMOP_VOCABULARY].[AoU_DRC].[local_source_to_concept_map] ec on ec.source_concept_id = person.ethnicity_concept_id     
  

       

--Visit occurrence 
if OBJECT_ID('OMOP_V5_Pcornet_V3.CAPMC_UCSD.visit_occurrence') is not null drop table OMOP_V5_Pcornet_V3.CAPMC_UCSD.visit_occurrence
select [visit_occurrence_id]
      ,cast(replace(PMI.[pmi id], 'P', '') as bigint) as [person_id]  --per DRC, Person_id = PMI_ID (remove 'P')
      ,[visit_concept_id]
      ,[visit_start_date]
      ,convert(nvarchar(30), [visit_start_date], 126) as [visit_start_datetime]
      ,[visit_end_date]
      ,convert(nvarchar(30), [visit_end_date], 126) as [visit_end_datetime]
      ,[visit_type_concept_id]
      ,[provider_id]
      ,[care_site_id]
      ,[visit_source_value]
      ,[visit_source_concept_id]
      ,[admitting_source_concept_id]	--A foreign key to the predefined concept in the Place of Service Vocabulary reflecting the admitting source for a visit.
      ,[admitting_source_value]			--The source code for the admitting source as it appears in the source data.
      ,[discharge_to_concept_id]		--A foreign key to the predefined concept in the Place of Service Vocabulary reflecting the discharge disposition for a visit.
      ,[discharge_to_source_value]		--The source code for the discharge disposition as it appears in the source data.
      ,NULL as [preceding_visit_occurrence_id]	--A foreign key to the VISIT_OCCURRENCE table of the visit immediately preceding this visit
     into OMOP_V5_Pcornet_V3.CAPMC_UCSD.visit_occurrence
       from omop5.visit_occurrence visit
        join #pmi pmi on pmi.person_id = visit.person_id 


-- Condition Occurrence
if OBJECT_ID('OMOP_V5_Pcornet_V3.CAPMC_UCSD.condition_occurrence') is not null drop table OMOP_V5_Pcornet_V3.CAPMC_UCSD.condition_occurrence
select [condition_occurrence_id]
      ,cast(replace(PMI.[pmi id], 'P', '') as bigint) as [person_id]  --per DRC, Person_id = PMI_ID (remove 'P')
      ,[condition_concept_id]
      ,[condition_start_date]
      ,convert(nvarchar(30), [condition_start_datetime], 126) as condition_start_datetime
      ,[condition_end_date]
      ,convert(nvarchar(30), [condition_end_datetime], 126) as condition_end_datetime
      ,[condition_type_concept_id]
      ,[stop_reason]
      ,0 as[provider_id]
      ,[visit_occurrence_id]
      ,[condition_source_value]
      ,[condition_source_concept_id]
      ,[condition_status_source_value]
      ,[condition_status_concept_id]
    	into OMOP_V5_Pcornet_V3.CAPMC_UCSD.condition_occurrence
        from omop5.condition_occurrence condition
       join #pmi pmi on pmi.person_id = condition.person_id 

    

-- Procedure Occurrence
if OBJECT_ID('OMOP_V5_Pcornet_V3.CAPMC_UCSD.procedure_occurrence') is not null drop table OMOP_V5_Pcornet_V3.CAPMC_UCSD.procedure_occurrence
select [procedure_occurrence_id]
      ,cast(replace(PMI.[pmi id], 'P', '') as bigint) as [person_id]  --per DRC, Person_id = PMI_ID (remove 'P')
      ,[procedure_concept_id]
      ,[procedure_date]
      ,CONVERT(nvarchar(30), [procedure_datetime], 126) as [procedure_datetime]
--      ,[procedure_datetime]
      ,[procedure_type_concept_id] 
      ,0 as [modifier_concept_id]
      ,[quantity]
      ,0 as [provider_id]
      ,[visit_occurrence_id]
      ,[procedure_source_value]
      ,[procedure_source_concept_id]
      ,[qualifier_source_value]     
        into OMOP_V5_Pcornet_V3.CAPMC_UCSD.procedure_occurrence
        from omop5.procedure_occurrence proced
      join #pmi pmi on pmi.person_id = proced.person_id 

        
-- Measurement 
if OBJECT_ID('OMOP_V5_Pcornet_V3.CAPMC_UCSD.measurement') is not null drop table OMOP_V5_Pcornet_V3.CAPMC_UCSD.measurement 
select  [measurement_id]
      ,cast(replace(PMI.[pmi id], 'P', '') as bigint) as [person_id]  --per DRC, Person_id = PMI_ID (remove 'P')
      ,[measurement_concept_id]
      ,[measurement_date]
      ,CONVERT(nvarchar(30), [measurement_datetime], 126) as [measurement_datetime]
      ,[measurement_type_concept_id]
      ,0 as [operator_concept_id]
      ,[value_as_number]
      ,0 as [value_as_concept_id]
      ,[unit_concept_id]
      ,[range_low]
      ,[range_high]
      ,0 as [provider_id]
      ,[visit_occurrence_id]
      ,[measurement_source_value]
      ,[measurement_source_concept_id]
      ,[unit_source_value]
      ,replace([value_source_value] , '"', '""') as [value_source_value]
        into OMOP_V5_Pcornet_V3.CAPMC_UCSD.measurement
        from omop5.measurement measurement
       join #pmi pmi on pmi.person_id = measurement.person_id 
        
        

-- Drug_Exposure
if OBJECT_ID('OMOP_V5_Pcornet_V3.CAPMC_UCSD.drug_exposure') is not null drop table OMOP_V5_Pcornet_V3.CAPMC_UCSD.drug_exposure 
select [drug_exposure_id]
      ,cast(replace(PMI.[pmi id], 'P', '') as bigint) as [person_id]  --per DRC, Person_id = PMI_ID (remove 'P')
      ,[drug_concept_id]
      ,[drug_exposure_start_date]
      ,CONVERT(nvarchar(30), [drug_exposure_start_datetime], 126) as [drug_exposure_start_datetime]
      ,[drug_exposure_end_date]
      ,CONVERT(nvarchar(30), cast([drug_exposure_end_datetime] as datetime), 126) as [drug_exposure_end_datetime]
      ,NULL as [verbatim_end_date]
      ,[drug_type_concept_id]
      ,[stop_reason]
      ,[refills]
      ,[quantity]
      ,[days_supply]
      ,[sig]
      ,[route_concept_id]
      ,[lot_number]
      , 0 as [provider_id]
      ,[visit_occurrence_id]
      ,[drug_source_value]
      ,[drug_source_concept_id]
      ,[route_source_value]
      ,[dose_unit_source_value]
        into OMOP_V5_Pcornet_V3.CAPMC_UCSD.drug_exposure
        from omop5.drug_exposure drug
       join #pmi pmi on pmi.person_id = drug.person_id 
        
select * from OMOP_V5_Pcornet_V3.CAPMC_UCSD.drug_exposure 


---------------------------


/***********************************************************
--	ETL from CAPMC identity tables to DRC PII Tables 
***********************************************************/

declare @OMOP_PERSON_CONCEPT_TYPE_ID bigint = 2000000812	--OMOP_PERSON_ID 
declare @PMI_ID_TYPE_CONCEPT_ID bigint = 2000000813 -- PMI_ID


Declare @HOMEPHONE_CONCEPT_TYPE_ID bigint = 2000000400 -- Home phone
Declare @WORKPHONE_CONCEPT_TYPE_ID bigint = 2000000401 -- Work phone
DECLARE @EMAIL_CONCEPT_TYPE_ID BIGINT =  2000000405 -- Email


--DRC PII Name table
truncate table CAPMC_UCSD.DRC_PII_NAME 
insert into CAPMC_UCSD.DRC_PII_NAME (
	PERSON_ID,
	FIRST_NAME ,
	MIDDLE_NAME ,
	LAST_NAME ,
	SUFFIX ,
	PREFIX 
)
select right(PMI.[PMI ID], (LEN(PMI.[PMI ID])-1)) as [PERSON_ID] --Acc to DRC: This is the PMI_ID (with the letter removed)
	, person.first_name  as [FIRST_NAME]
	, person.middle_name as [MIDDLE_NAME]
	, person.surname as [LAST_NAME]
	, coalesce(person.qualifications, person.honorifics) as [SUFFIX]
	, person.title as [PREFIX]
from omop5.phi_person person
JOIN #PMI PMI ON PMI.MASTER_ID = person.master_id 
--join omop5.phi_identifier iden on iden.master_id = person.master_id 
--	and iden.id_type_concept_id = @PMI_ID_TYPE_CONCEPT_ID -- PMI_ID
where pmi.master_id is not null 
group by PMI.[PMI ID], person.first_name, person.middle_name, person.surname, person.qualifications, person.honorifics, person.title


	
	----------
	
--DRC PII Email table
truncate table CAPMC_UCSD.DRC_PII_EMAIL 
insert into CAPMC_UCSD.DRC_PII_EMAIL (
	PERSON_ID ,
	EMAIL 
)	
select right(PMI.[PMI ID], (LEN(PMI.[PMI ID])-1)) as [PERSON_ID] --Acc to DRC: This is the PMI_ID (with the letter removed)
	, email.value  as [EMAIL]
from omop5.phi_telecom email
JOIN #PMI PMI ON PMI.MASTER_ID = email.master_id
--join omop5.phi_identifier iden on iden.master_id = email.master_id 
--	and iden.id_type_concept_id = @PMI_ID_TYPE_CONCEPT_ID
where email.use_concept_id = @EMAIL_CONCEPT_TYPE_ID
and pmi.master_id is not null 
group by PMI.[PMI ID], email.value




--DRC PII Phone Number table
truncate table CAPMC_UCSD.DRC_PII_PHONE_NUMBER 
insert into CAPMC_UCSD.DRC_PII_PHONE_NUMBER(
	PERSON_ID,
	PHONE_NUMBER 
)	
select right(PMI.[PMI ID], (LEN(PMI.[PMI ID])-1)) as [PERSON_ID] --Acc to DRC: This is the PMI_ID (with the letter removed)
	, Phone.value  as [PHONE_NUMBER]
from omop5.phi_telecom Phone
JOIN #PMI PMI ON PMI.MASTER_ID = Phone.master_id
--join omop5.phi_identifier iden on iden.master_id = Phone.master_id 
--	and iden.id_type_concept_id = @PMI_ID_TYPE_CONCEPT_ID
where Phone.use_concept_id in ( @HOMEPHONE_CONCEPT_TYPE_ID,@WORKPHONE_CONCEPT_TYPE_ID		-- Work phone
		) and pmi.master_id is not null 
group by PMI.[PMI ID], phone.value






----DRC PII Address table
truncate table CAPMC_UCSD.DRC_PII_ADDRESS 
insert into CAPMC_UCSD.DRC_PII_ADDRESS(
	PERSON_ID ,
	LOCATION_ID 
)	
select right(PMI.[PMI ID], (LEN(PMI.[PMI ID])-1))  as [PERSON_ID] --Acc to DRC: This is the PMI_ID (with the letter removed)
	, per.location_id as [LOCATION_ID]
from omop5.person per 
JOIN #PMI PMI ON PMI.PERSON_ID = per.person_id
--join omop5.phi_identifier OMOP_Person_id on per.person_id = OMOP_Person_id.identifier  
--	and OMOP_Person_id.id_type_concept_id = @OMOP_PERSON_CONCEPT_TYPE_ID -- OMOP_Person_id
--join omop5.phi_identifier iden on iden.master_id = OMOP_Person_id.master_id 
--	and iden.id_type_concept_id = @PMI_ID_TYPE_CONCEPT_ID -- PMI_ID
where pmi.master_id is not null 
group by PMI.[PMI ID], per.location_id



--DRC PII MRN table
truncate table CAPMC_UCSD.DRC_PII_MRN 
insert into CAPMC_UCSD.DRC_PII_MRN (
	PERSON_ID ,
	HEALTH_SYSTEM ,
	MRN 
)
select right(PMI.[PMI ID], (LEN(PMI.[PMI ID])-1)) as [PERSON_ID] --Acc to DRC: This is the PMI_ID (with the letter removed)
	, person.source_system as [HEALTH_SYSTEM]
	, person.mrn  as [MRN]
from omop5.phi_person person
JOIN #PMI PMI ON PMI.MASTER_ID = person.master_id
--join omop5.phi_identifier iden on iden.master_id = person.master_id 
--	and iden.id_type_concept_id = @PMI_ID_TYPE_CONCEPT_ID 
where  pmi.master_id is not null 
group by PMI.[PMI ID], person.source_system, person.mrn
© 2018 GitHub, Inc.