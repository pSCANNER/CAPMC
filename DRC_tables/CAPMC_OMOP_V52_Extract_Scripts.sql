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

declare @OMOP_PERSON_CONCEPT_TYPE_ID bigint = 2000000812	--OMOP_PERSON_ID 
declare @PMI_ID_TYPE_CONCEPT_ID bigint = 2000000813 -- PMI_ID


--Person table
select cast(replace(PMI_ID.identifier, 'P', '') as bigint) as [person_id]  --per DRC, Person_id = PMI_ID (remove 'P')
      ,[gender_concept_id]
      ,[year_of_birth]
      ,[month_of_birth]
      ,[day_of_birth]
      ,DATEFROMPARTS ( [year_of_birth], [month_of_birth], [day_of_birth] )    birth_datetime
      ,[race_concept_id]
      ,[ethnicity_concept_id]
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
       from omop5.person person
       join omop5.phi_identifier omop_id on omop_id.identifier = person.person_id 
				and omop_id.id_type_concept_id = @OMOP_PERSON_CONCEPT_TYPE_ID
	   join omop5.phi_identifier PMI_ID on PMI_ID.master_id = omop_id.master_id
				and PMI_ID.id_type_concept_id = @PMI_ID_TYPE_CONCEPT_ID
       


--Visit occurrence 
select [visit_occurrence_id]
      ,cast(replace(PMI_ID.identifier, 'P', '') as bigint) as [person_id]  --per DRC, Person_id = PMI_ID (remove 'P')
      ,[visit_concept_id]
      ,[visit_start_date]
      ,[visit_start_date] as [visit_start_datetime]
      ,[visit_end_date]
      ,[visit_end_date] as [visit_end_datetime]
      ,[visit_type_concept_id]
      ,[provider_id]
      ,[care_site_id]
      ,[visit_source_value]
      ,[visit_source_concept_id]
      ,[admitting_source_concept_id]	--A foreign key to the predefined concept in the Place of Service Vocabulary reflecting the admitting source for a visit.
      ,[admitting_source_value]			--The source code for the admitting source as it appears in the source data.
      ,[discharge_to_concept_id]		--A foreign key to the predefined concept in the Place of Service Vocabulary reflecting the discharge disposition for a visit.
      ,[discharge_to_source_value]		--The source code for the discharge disposition as it appears in the source data.
      ,[preceding_visit_occurrence_id]	--A foreign key to the VISIT_OCCURRENCE table of the visit immediately preceding this visit
       from omop5.visit_occurrence visit
       join omop5.phi_identifier omop_id on omop_id.identifier = visit.person_id 
				and omop_id.id_type_concept_id = @OMOP_PERSON_CONCEPT_TYPE_ID
	   join omop5.phi_identifier PMI_ID on PMI_ID.master_id = omop_id.master_id
				and PMI_ID.id_type_concept_id = @PMI_ID_TYPE_CONCEPT_ID




-- Condition Occurrence
select [condition_occurrence_id]
      ,cast(replace(PMI_ID.identifier, 'P', '') as bigint) as [person_id]  --per DRC, Person_id = PMI_ID (remove 'P')
      ,[condition_concept_id]
      ,[condition_start_date]
      ,[condition_start_datetime]
      ,[condition_end_date]
      ,[condition_end_datetime]
      ,[condition_type_concept_id]
      ,[stop_reason]
      ,0 as[provider_id]
      ,[visit_occurrence_id]
      ,[condition_source_value]
      ,[condition_source_concept_id]
      ,[condition_status_source_value]
      ,[condition_status_concept_id]
        from omop5.condition_occurrence condition
       join omop5.phi_identifier omop_id on omop_id.identifier = condition.person_id 
				and omop_id.id_type_concept_id = @OMOP_PERSON_CONCEPT_TYPE_ID
	   join omop5.phi_identifier PMI_ID on PMI_ID.master_id = omop_id.master_id
				and PMI_ID.id_type_concept_id = @PMI_ID_TYPE_CONCEPT_ID
    



-- Procedure Occurrence
select [procedure_occurrence_id]
      ,cast(replace(PMI_ID.identifier, 'P', '') as bigint) as [person_id]  --per DRC, Person_id = PMI_ID (remove 'P')
      ,[procedure_concept_id]
      ,[procedure_date]
      ,[procedure_datetime]
      ,[procedure_type_concept_id]
      ,0 as [modifier_concept_id]
      ,[quantity]
      ,0 as [provider_id]
      ,[visit_occurrence_id]
      ,[procedure_source_value]
      ,[procedure_source_concept_id]
      ,[qualifier_source_value]     
        from omop5.procedure_occurrence proced
       join omop5.phi_identifier omop_id on omop_id.identifier = proced.person_id 
				and omop_id.id_type_concept_id = @OMOP_PERSON_CONCEPT_TYPE_ID
	   join omop5.phi_identifier PMI_ID on PMI_ID.master_id = omop_id.master_id
				and PMI_ID.id_type_concept_id = @PMI_ID_TYPE_CONCEPT_ID
        

      
-- Measurement 
select  [measurement_id]
      ,cast(replace(PMI_ID.identifier, 'P', '') as bigint) as [person_id]  --per DRC, Person_id = PMI_ID (remove 'P')
      ,[measurement_concept_id]
      ,[measurement_date]
      ,[measurement_datetime]
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
      ,[value_source_value]  
        from omop5.measurement measurement
       join omop5.phi_identifier omop_id on omop_id.identifier = measurement.person_id 
				and omop_id.id_type_concept_id = @OMOP_PERSON_CONCEPT_TYPE_ID
	   join omop5.phi_identifier PMI_ID on PMI_ID.master_id = omop_id.master_id
				and PMI_ID.id_type_concept_id = @PMI_ID_TYPE_CONCEPT_ID



-- Drug_Exposure
select [drug_exposure_id]
      ,cast(replace(PMI_ID.identifier, 'P', '') as bigint) as [person_id]  --per DRC, Person_id = PMI_ID (remove 'P')
      ,[drug_concept_id]
      ,[drug_exposure_start_date]
      ,[drug_exposure_start_datetime]
      ,[drug_exposure_end_date]
      ,[drug_exposure_end_datetime]
      ,[verbatim_end_date]
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
        from omop5.drug_exposure drug
       join omop5.phi_identifier omop_id on omop_id.identifier = drug.person_id 
				and omop_id.id_type_concept_id = @OMOP_PERSON_CONCEPT_TYPE_ID
	   join omop5.phi_identifier PMI_ID on PMI_ID.master_id = omop_id.master_id
				and PMI_ID.id_type_concept_id = @PMI_ID_TYPE_CONCEPT_ID

