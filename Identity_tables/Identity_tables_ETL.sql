/*****************************************************************************
-- Author: Paulina Paul
-- Create date: 03/21/2018
-- Description: Implement identity tables for CAPMC
-- Tables:	
--		PHI_PERSON
--		PHI_IDENTIFIER (OMOP PERSON_ID, MRN, PMI_ID and REDCAP STUDY_ID)
--		PHI_TELCOM (HOME PHONE, WORK PHONE and EMAIL)
--		PHI_ADDRESS

-- User action: Update values for variables reflecting values corresponding to each site.
--				Please refer to Identity_Tables_Vocabulary_additions.xlsx

*****************************************************************************/
use OMOP_V5_Pcornet_V3
go



--storing clarity data in temp table for performance
if OBJECT_ID ('tempdb.dbo.#pat_id') is not null drop table #pat_id 
select * into #pat_Id from openquery ([hs-eclarity-v], 

'select p.pat_id, ii.identity_id as MRN, p.pat_first_name,  p.pat_middle_name, p.pat_last_name, p.pat_title_c, p.pat_name_suffix_c, p2.MAIDEN_NAME, 

p.add_line_1, p.add_line_2, p.city, p.state_c, p.zip, p.county_c, p.country_c,  p.home_phone, p.work_phone, p.email_address, p.pat_status_c, 
p.birth_date, p.sex_c, p.ethnic_group_c, p.marital_status_c, p.religion_c, p.language_c, p.SSn,  ii_master.identity_id as Epic_internal_Id,
p.death_date, p.CUR_PCP_PROV_ID, p3.MOTHER_MAIDEN_NAME, vp.is_valid_pat_YN, p.EMPLOYER_ID, p.LANG_CARE_C, zcl_pref.name as pref_language, 
 

title.name as Title, zms.name as Marital_status, zcl.name as Language

from clarity_prod.dbo.patient p
join clarity_prod.dbo.service_area_id sa on sa.pat_id = p.pat_id and sa.SERVICE_AREA_ID = 10 -- UCSD patients only
 join clarity_prod.dbo.identity_id ii on ii.pat_id = p.pat_id  and ii.identity_type_id = 2 --different for each institution
 join clarity_prod.dbo.patient_2 p2 on p2.pat_id = p.pat_id 
 join clarity_prod.dbo.patient_3 p3 on p3.pat_id = p.pat_id 
 
left join clarity_prod.dbo.identity_id ii_master on ii_master.pat_id = p.pat_id  and ii_master.identity_type_id = 0 --different for each institution
left join clarity_prod.dbo.valid_patient vp on vp.pat_id = p.pat_id 
left join clarity_prod.dbo.ZC_PAT_TITLE title on title.PAT_TITLE_C = p.PAT_TITLE_C
left join clarity_prod.dbo.zc_marital_status zms on zms.marital_status_c= p.marital_status_c 
left join clarity_prod.dbo.zc_language zcl on zcl.language_c= p.language_c 
left join clarity_prod.dbo.zc_language zcl_pref on zcl_pref.language_c= p.LANG_CARE_C 

--left outer join clarity_prod.dbo.ZC_PATIENT_RACE prc on r.PATIENT_RACE_C = prc.PATIENT_RACE_C
--left outer join clarity_prod.dbo.ZC_ETHNIC_GROUP eg on p.ETHNIC_GROUP_C = eg.ETHNIC_GROUP_C
--left outer join clarity_prod.dbo.ZC_SEX sx on p.SEX_C = sx.RCPT_MEM_SEX_C')




-- Table containing HealthPro exported patient data mapped to clarity patient
select [PMI ID], pat_id  from #PMI_patient_mapping  

--Table containing Redcap study_ids
select study_id, pat_id from #redcap

-------------------------------------------------

DECLARE @ASSIGNER VARCHAR(20) = 'UCSD'
DECLARE @SOURCE_SYTEM VARCHAR(20) = 'UCSD Epic'

--PHI Person 
insert into [OMOP_V5_Pcornet_V3].[omop5].[phi_person] (
--	[phi_person_id]  auto-increment field
	[master_id]
     ,[active]
     ,[cdm_id]
     ,[ssn]
     ,[mrn]
     ,[surname]
     ,[first_name]
     ,[title]
     ,[middle_name]
     ,[nickname]
     ,[qualifications]
     ,[honorifics]
     ,[maiden_name]
     ,[employer]
     ,[employer_period_start_date]
     ,[employer_period_end_date]
     ,[gender_concept_id]
     ,[gender_source_value]
     ,[gender_source_concept_id]
     ,[race_concept_id]
     ,[race_source_value]
     ,[race_source_concept_id]
     ,[ethnicity_concept_id]
     ,[ethnicity_source_value]
     ,[ethnicity_source_concept_id]
     ,[birth_date]
     ,[birthplace]
     ,[deceased]
     ,[deceased_datetime]
     ,[marital_status_concept_id]
     ,[marital_status_source_value]
     ,[marital_status_source_concept_id]
     ,[mothers_maiden_name]
     ,[multiple_birth_boolean]
     ,[multiple_birth_integer]
     ,[photo]
     ,[language_concept_id]
     ,[language_source_value]
     ,[language_source_concept_id]
     ,[preferred_language_concept_id]
     ,[preferred_language_source_value]
     ,[preferred_language_source_concept_id]
     ,[general_practitioner]
     ,[managing_organization]
     ,[source_system]
 )
select 
	   pid.Epic_Internal_id as [Master_ID]
	   --pid.pat_id as [Master_ID]
	  ,case when pid.is_valid_pat_YN = 'Y' then 1 else 0 end as [active]
	  ,person.person_id as [cdm_id]
      ,pid.SSN as [ssn]
      ,pid.MRN as [mrn]
      ,pid.pat_last_name as [surname]
      ,pid.pat_first_name as [first_name]
      ,pid.title as [title]
      ,pid.pat_middle_name as [middle_name]
      ,NULL as [nickname]
      ,NULL as [qualifications]
      ,NULL as [honorifics]
      ,pid.MAIDEN_NAME as [maiden_name]
      ,pid.EMPLOYER_ID as [employer]
      ,NULL as [employer_period_start_date]
      ,NULL as [employer_period_end_date]
      ,person.gender_concept_id as [gender_concept_id]
      ,person.gender_source_value as [gender_source_value]
      ,person.gender_source_concept_id as [gender_source_concept_id]
      ,person.race_concept_id as  [race_concept_id]
      ,person.race_source_value as [race_source_value]
      ,person.race_source_concept_id as [race_source_concept_id]
      ,person.ethnicity_concept_id as [ethnicity_concept_id]
      ,person.ethnicity_source_value as [ethnicity_source_value]
      ,person.ethnicity_source_concept_id as [ethnicity_source_concept_id]
      ,pid.birth_date as [birth_datetime]
      ,NULL as [birthplace]
      ,case when d.person_id is not null then 'Y' else 'N' end as [deceased]
      ,d.[death_datetime] as [death_datetime]
      ,NULL as [marital_status_concept_id]
      ,pid.Marital_Status as  [marital_status_source_value]
      ,NULL as [marital_status_source_concept_id]
      ,pid.Mother_maiden_name as [mothers_maiden_name]
      ,NULL as [multiple_birth_boolean]
      ,NULL as [multiple_birth_integer]
      ,NULL as [photo]
      ,NULL as [language_concept_id]
      ,pid.[language] as [language_source_value]
      ,NULL as [language_source_concept_id]
      ,NULL as [preferred_language_concept_id]
      ,pid.pref_language as [preferred_language_source_value]
      ,NULL as [preferred_language_source_concept_id]
      ,prov.provider_id as [general_practitioner]
      ,@ASSIGNER as [managing_organization]
      ,@SOURCE_SYTEM as [source_system]
from #pat_id pid 
join link5.pat_link_active_view pla on pla.pat_id = pid.pat_id -- site specific patient ID mapping
join omop5.person person on person.person_id = pla.person_id 
left join omop5.death d on d.person_id = person.person_id 
left join omop5.provider prov on prov.provider_source_value = pid.cur_pcp_prov_id 



-------------------------------------------------------------------------------------------------------------

-- PHI Identifier table  
-- (OMOP ID, HealthPro PMI ID only, Study ID (Redcap Issued Auto-increment ID), EHR MRN)

---------------------------------------------------------------------------------------------------------------

-- pSCANNER OMOP_ID: Change values for the following corresponding to each site.
-- pSCANNER OMOP ID is the preferred record
declare @id_use_source_value_pScannerOMOP varchar(100) = 'pSCANNER OMOP'
declare @id_type_source_value_pScannerOMOP varchar(100) = 'OMOP Person ID' 
declare @system_pScannerOMOP varchar(100) = 'pSCANNER - UCSD'
declare @assigner_pScannerOMOP varchar(100) = 'pSCANNER - UCSD'
declare @concept_code_pScannerOMOP varchar(100) = 'pSCANNER_OMOP_UCSD'   --concept_class_id = identifier use


insert into [OMOP_V5_Pcornet_V3].[omop5].[phi_identifier] (
 	--[phi_identifier_id], auto-increment field
      [preferred_record]
      ,[master_id]
      ,[source_id]
      ,[id_use_concept_id]
      ,[id_use_source_value]
      ,[id_use_source_concept_id]
      ,[identifier]
      ,[id_type_concept_id]
      ,[id_type_source_value]
      ,[id_type_source_concept_id]
      ,[system]
      ,[period_start_date]
      ,[period_end_date]
      ,[assigner]
     )
select 
	  1 as [preferred_record]
	, phi.master_id [master_id]
	, pla.pat_id as [source_id]
    , id_use.concept_id as [id_use_concept_id]
    , @id_use_source_value_pScannerOMOP as [id_use_source_value]
    , id_use.concept_id as [id_use_source_concept_id]
    , person.person_id as [identifier]
    , id_type.concept_id as [id_type_concept_id]
    , @id_type_source_value_pScannerOMOP as [id_type_source_value]
    , id_type.concept_id  as [id_type_source_concept_id]
    , @system_pScannerOMOP as [system]
    , '1970-01-01' as [period_start_date]
    , '2099-12-31' as [period_end_date]
    , @assigner_pScannerOMOP as [assigner]
from  omop5.person person			
join link5.pat_link_active_view pla on pla.person_id = person.person_id -- site specific patient ID mapping
join omop5.phi_person phi on phi.cdm_id= pla.person_id
left join omop_vocabulary.vocab5.concept id_use on id_use.vocabulary_id = 'CAPMC' and id_use.concept_class_id = 'Identifier Use'
		and id_use.concept_code = @concept_code_pScannerOMOP and id_use.invalid_reason is NULL
left join omop_vocabulary.vocab5.concept id_type on id_type.vocabulary_id = 'CAPMC' and id_type.concept_class_id = 'Identifier Type'
		and id_type.concept_code = 'OMOP_PERSON_ID' and id_type.invalid_reason is NULL
 group by phi.master_id, pla.pat_id, id_use.CONCEPT_ID, id_type.CONCEPT_ID, person.person_id 		
		
--------------------------------------------------------------------------
 
 



--EHR MRN (MEDCIAL RECORD NUMBER): Change values for the following corresponding to each site.
declare @id_use_source_value_MRN varchar(100) = 'UCSD EHR MRN'
declare @id_type_source_value_MRN varchar(100) = 'Medical Record Number'
declare @system_MRN varchar(100) = 'UCSD Epic'
declare @assigner_MRN varchar(100) = 'UCSD EHR'
declare @concept_code_MRN varchar(100) = 'EHR_MRN_UCSD'   --concept_class_id = identifier use

insert into [OMOP_V5_Pcornet_V3].[omop5].[phi_identifier] (
 	--[phi_identifier_id], auto-increment field
      [preferred_record]
      ,[master_id]
      ,[source_id]
      ,[id_use_concept_id]
      ,[id_use_source_value]
      ,[id_use_source_concept_id]
      ,[identifier]
      ,[id_type_concept_id]
      ,[id_type_source_value]
      ,[id_type_source_concept_id]
      ,[system]
      ,[period_start_date]
      ,[period_end_date]
      ,[assigner]
     )
select 
	  0 as [preferred_record]
	, phi.master_id [master_id]
	, ii.pat_id as [source_id]
    , id_use.concept_id as [id_use_concept_id]
    , @id_use_source_value_MRN as [id_use_source_value] --Change for each site
    , id_use.concept_id as [id_use_source_concept_id]
    , phi.mrn as [identifier]
    , id_type.concept_id as [id_type_concept_id]
    , @id_type_source_value_MRN as [id_type_source_value]
    , id_type.concept_id  as [id_type_source_concept_id]
    , @system_MRN as [system]							--Change for each site
    , '1970-01-01' as [period_start_date]
    , '2099-12-31' as [period_end_date]
    , @assigner_MRN as [assigner]
from [CDWRMAP-ENCRYPT].dbo.identity_id ii			--clarity MRN table
join link5.pat_link_active_view pla on pla.pat_id = ii.pat_id -- site specific patient ID mapping
join omop5.phi_person phi on phi.cdm_id= pla.person_id
left join omop_vocabulary.vocab5.concept id_use on id_use.vocabulary_id = 'CAPMC' and id_use.concept_class_id = 'Identifier Use'
		and id_use.concept_code = @concept_code_MRN and id_use.invalid_reason is NULL			
left join omop_vocabulary.vocab5.concept id_type on id_type.vocabulary_id = 'CAPMC' and id_type.concept_class_id = 'Identifier Type'
		and id_type.concept_code = 'MR' and id_type.invalid_reason is NULL
 group by phi.master_id, ii.pat_id, id_use.CONCEPT_ID, id_type.CONCEPT_ID, phi.mrn 
 
 
 
	------------------------------------------
	
	
	
		
-- HeatlhPro PMI_ID: Change values for the following corresponding to each site.
declare @id_use_source_value_PMI varchar(100) = 'All of Us'
declare @id_type_source_value_PMI varchar(100) = 'PMI_ID' 
declare @system_PMI varchar(100) = 'HealthPro'
declare @assigner_PMI varchar(100) = 'HealthPro'
declare @concept_code_PMI varchar(100) = 'AoU'

insert into [OMOP_V5_Pcornet_V3].[omop5].[phi_identifier] (
 	--[phi_identifier_id], auto-increment field
      [preferred_record]
      ,[master_id]
      ,[source_id]
      ,[id_use_concept_id]
      ,[id_use_source_value]
      ,[id_use_source_concept_id]
      ,[identifier]
      ,[id_type_concept_id]
      ,[id_type_source_value]
      ,[id_type_source_concept_id]
      ,[system]
      ,[period_start_date]
      ,[period_end_date]
      ,[assigner]
     )
select 
	  0 as [preferred_record]
	, phi.master_id [master_id]
	, PMI.pat_id as [source_id]
    , id_use.concept_id as [id_use_concept_id]
    , @id_use_source_value_PMI as [id_use_source_value]
    , id_use.concept_id as [id_use_source_concept_id]
    , PMI.PMI_ID as [identifier]
    , id_type.concept_id as [id_type_concept_id]
    , @id_type_source_value_PMI as [id_type_source_value]
    , id_type.concept_id  as [id_type_source_concept_id]
    , @system_PMI as [system]
    , '1970-01-01' as [period_start_date]
    , '2099-12-31' as [period_end_date]
    , @assigner_PMI as [assigner]
from  #PMI_patient_mapping PMI			--Downloaded from HealthPro & mapped to EHR patient
join omop5.phi_identifier phi on phi.source_id = PMI.pat_id 
left join omop_vocabulary.vocab5.concept id_use on id_use.vocabulary_id = 'CAPMC' and id_use.concept_class_id = 'Identifier Use'
		and id_use.concept_code = @concept_code_PMI and id_use.invalid_reason is NULL
left join omop_vocabulary.vocab5.concept id_type on id_type.vocabulary_id = 'CAPMC' and id_type.concept_class_id = 'Identifier Type'
		and id_type.concept_code = 'PMI_ID' and id_type.invalid_reason is NULL
  group by phi.master_id, PMI.pat_id , id_use.CONCEPT_ID, id_type.CONCEPT_ID, PMI.PMI_ID

 
 
 
------------------------------------------------------------


-- Redcap Study_ID: Change values for the following corresponding to each site.
declare @id_use_source_value_redcap varchar(100) = 'CAPMC Redcap Study ID'
declare @id_type_source_value_redcap varchar(100) = 'Redcap Study ID' 
declare @system_redcap varchar(100) = 'UCSD Redcap'
declare @assigner_redcap varchar(100) = 'UCSD Redcap'
declare @concept_code_redcap varchar(100) = 'REDCAP_STUDYID_UCSD'

insert into [OMOP_V5_Pcornet_V3].[omop5].[phi_identifier] (
 	--[phi_identifier_id], auto-increment field
      [preferred_record]
      ,[master_id]
      ,[source_id]
      ,[id_use_concept_id]
      ,[id_use_source_value]
      ,[id_use_source_concept_id]
      ,[identifier]
      ,[id_type_concept_id]
      ,[id_type_source_value]
      ,[id_type_source_concept_id]
      ,[system]
      ,[period_start_date]
      ,[period_end_date]
      ,[assigner]
     )
select 
	  0 as [preferred_record]
	, phi.master_id [master_id]
	, redcap.pat_id as [source_id]
    , id_use.concept_id as [id_use_concept_id]
    , @id_use_source_value_redcap as [id_use_source_value]
    , id_use.concept_id as [id_use_source_concept_id]
    , redcap.Study_ID as [identifier]
    , id_type.concept_id as [id_type_concept_id]
    , @id_type_source_value_redcap as [id_type_source_value]
    , id_type.concept_id  as [id_type_source_concept_id]
    , @system_redcap as [system]
    , '1970-01-01' as [period_start_date]
    , '2099-12-31' as [period_end_date]
    , @assigner_redcap as [assigner]
from  #Redcap redcap			--Downloaded from Redcap & mapped to EHR patient
join omop5.phi_identifier phi on phi.source_id= redcap.pat_id 
left join omop_vocabulary.vocab5.concept id_use on id_use.vocabulary_id = 'CAPMC' and id_use.concept_class_id = 'Identifier Use'
		and id_use.concept_code = @concept_code_redcap and id_use.invalid_reason is NULL
left join omop_vocabulary.vocab5.concept id_type on id_type.vocabulary_id = 'CAPMC' and id_type.concept_class_id = 'Identifier Type'
		and id_type.concept_code = 'STUDY_ID' and id_type.invalid_reason is NULL
  group by phi.master_id, redcap.pat_id , id_use.CONCEPT_ID, id_type.CONCEPT_ID, redcap.Study_ID


 
-----------------------------------------

 --PHI_Telecom
-----------------------------------------
   
--Home phone  
--Change values for the following corresponding to site.
declare @system_source_value_HomePhone varchar(100) = 'UCSD EHR'
declare @system_concept_code_HomePhone varchar(100) = 'EHR_UCSD'


insert into OMOP_V5_Pcornet_V3.omop5.phi_telecom 
select --  [phi_telecom_id], auto_increment
        p.Epic_Internal_id as [master_id]
      , systemConcept.concept_id  as [system_concept_id]
      , @system_source_value_HomePhone [system_source_value]
      , systemConcept.concept_id as [system_source_concept_id]
      , p.HOME_PHONE as [value]
      , useConcept.concept_id as [use_concept_id]
      , 'HOME PHONE' as [use_source_value]
      , useConcept.concept_id as [use_source_concept_id]
      , NULL as [rank]
      , NULL as [preferred_record]
      , '1970-01-01' as  [telcom_start_date]
      , '2099-12-31' as [telcom_end_date]
    from  #pat_id  p
	LEFT join  OMOP_VOCABULARY.vocab5.concept systemConcept on systemConcept.vocabulary_id = 'CAPMC'
		and systemConcept.concept_class_id = 'System ID' and systemConcept.concept_code = @system_concept_code_HomePhone
	left join OMOP_VOCABULARY.vocab5.concept useConcept on useConcept.vocabulary_id = 'CAPMC'
		and useConcept.concept_class_id = 'Telecom use' and useConcept.concept_code = 'Home'
	where p.home_phone is not null 
	group by p.Epic_Internal_id , systemConcept.concept_id , p.HOME_PHONE ,useConcept.concept_id 
	
	
				-----------------------------------------
	

--Work phone  
--Change values for the following corresponding to site.
declare @system_source_value_WorkPhone varchar(100) = 'UCSD EHR'
declare @system_concept_code_WorkPhone varchar(100) = 'EHR_UCSD'



insert into OMOP_V5_Pcornet_V3.omop5.phi_telecom 
select --  [phi_telecom_id], auto_increment
        p.Epic_Internal_id as [master_id]
      , systemConcept.concept_id  as [system_concept_id]
      , @system_source_value_WorkPhone [system_source_value]
      , systemConcept.concept_id as [system_source_concept_id]
      , p.WORK_PHONE as [value]
      , useConcept.concept_id as [use_concept_id]
      , 'WORK PHONE' as [use_source_value]
      , useConcept.concept_id as [use_source_concept_id]
      , NULL as [rank]
      , NULL as [preferred_record]
      , '1970-01-01' as  [telcom_start_date]
      , '2099-12-31' as [telcom_end_date]
    from  #pat_id  p
	LEFT join  OMOP_VOCABULARY.vocab5.concept systemConcept on systemConcept.vocabulary_id = 'CAPMC'
		and systemConcept.concept_class_id = 'System ID' and systemConcept.concept_code = @system_concept_code_WorkPhone
	left join OMOP_VOCABULARY.vocab5.concept useConcept on useConcept.vocabulary_id = 'CAPMC'
		and useConcept.concept_class_id = 'Telecom use' and useConcept.concept_code = 'Work'
	where p.WORK_PHONE is not null 
	group by p.Epic_Internal_id , systemConcept.concept_id , p.WORK_PHONE ,useConcept.concept_id 
	
		


				-----------------------------------------



--Email Address 
--Change values for the following corresponding to site.
declare @system_source_value_Email varchar(100) = 'UCSD EHR'
declare @system_concept_code_Email varchar(100) = 'EHR_UCSD'



insert into OMOP_V5_Pcornet_V3.omop5.phi_telecom 
select --  [phi_telecom_id], auto_increment
        p.Epic_Internal_id as [master_id]
      , systemConcept.concept_id  as [system_concept_id]
      , @system_concept_code_Email [system_source_value]
      , systemConcept.concept_id as [system_source_concept_id]
      , p.EMAIL_ADDRESS as [value]
      , useConcept.concept_id as [use_concept_id]
      , 'Email Address' as [use_source_value]
      , useConcept.concept_id as [use_source_concept_id]
      , NULL as [rank]
      , NULL as [preferred_record]
      , '1970-01-01' as  [telcom_start_date]
      , '2099-12-31' as [telcom_end_date]
    from  #pat_id  p
	LEFT join  OMOP_VOCABULARY.vocab5.concept systemConcept on systemConcept.vocabulary_id = 'CAPMC'
		and systemConcept.concept_class_id = 'System ID' and systemConcept.concept_code = @system_concept_code_Email
	left join OMOP_VOCABULARY.vocab5.concept useConcept on useConcept.vocabulary_id = 'CAPMC'
		and useConcept.concept_class_id = 'Telecom use' and useConcept.concept_code = 'Email'
	where p.EMAIL_ADDRESS is not null 
	group by p.Epic_Internal_id , systemConcept.concept_id ,p.EMAIL_ADDRESS ,useConcept.concept_id 
	
		


				-----------------------------------------

--PHI_LOCATION

--Home address
declare @use_source_value_homeAddr varchar(100) = 'Postal Address'
declare @type_source_value varchar(100) = 'Both Postal and Physical Address'


INSERT INTO [OMOP_V5_Pcornet_V3].[omop5].[phi_location] (
--		[phi_location_id] Auto-increment
      --,
      [master_id]
      ,[use_concept_id]
      ,[use_source_value]
      ,[use_source_concept_id]
      ,[type_concept_id]
      ,[type_source_value]
      ,[type_source_concept_id]
      ,[value]
      ,[address_line_1]
      ,[address_line_2]
      ,[city]
      ,[district]
      ,[state]
      ,[postal_code]
      ,[country]
      ,[preferred_record]
      ,[period_start_date]
      ,[period_end_date])

select --[phi_location_id] Auto-increment
      --,
      p.Epic_Internal_id as [master_id]
      , useConcept.concept_id AS [use_concept_id]
      , @use_source_value_homeAddr AS [use_source_value]
      , useConcept.concept_id AS [use_source_concept_id]
      , typeConcept.concept_id AS [type_concept_id]
      , @type_source_value AS [type_source_value]
      , typeConcept.concept_id AS [type_source_concept_id]
      , left(concat (p.add_line_1 , ' ', p.add_Line_2 ,' ', p.city ,' ',  zcc.name ,' ',  zs.name ,' ', p.zip ,' ', zc.name), 50)
			AS [value]
      , left(p.add_line_1, 50) as  [address_line_1]
      , left(p.add_line_2,50) as [address_line_2]
      , left(p.city, 50) as [city]
      , left(zcc.name,50) as [district]   --county
      , left(zs.NAME, 50) as [state]
      , left(p.zip, 50)  as [postal_code]
      , left(zc.NAME, 50) as [country]
      , '1' [preferred_record]
      , '1970-01-01' as [period_start_date]
      , '2099-12-31' as [period_end_date]
  FROM #pat_id  p
  left join CDWRPROD_CLARITY.dbo.ZC_COUNTY zcc on zcc.COUNTY_C = p.county_c 
  left join CDWRPROD_CLARITY.dbo.ZC_STATE zs on zs.STATE_C = p.state_c 
  left join CDWRPROD_CLARITY.dbo.zc_country zc on zc.country_c  = p.country_c 
		
  left join OMOP_VOCABULARY.vocab5.concept useConcept on useConcept.vocabulary_id = 'CAPMC'
		and useConcept.concept_class_id = 'Address Use' and useConcept.concept_code = 'PostalAddressUse'
		
  left join OMOP_VOCABULARY.vocab5.concept typeConcept on typeConcept.vocabulary_id = 'CAPMC'
		and typeConcept.concept_class_id = 'Address Type' and typeConcept.concept_code = 'Both'
   
WHERE P.ADD_LINE_1 IS NOT NULL 
group by p.Epic_Internal_id , useConcept.concept_id , typeConcept.concept_id, p.add_line_1, p.add_line_2, p.city, zcc.NAME, zs.NAME, p.zip, zc.NAME


-----------------------------------------


