

--Add values to the Vocabulary tables

--Domain table
insert into OMOP_VOCABULARY.vocab5.DOMAIN (DOMAIN_ID, DOMAIN_NAME, DOMAIN_CONCEPT_ID)
select 'Identity Management', 'Protected Health Information Identifiers', 2000000000 as domain_concept_id 


--Vocabulary table
insert into OMOP_VOCABULARY.vocab5.VOCABULARY (VOCABULARY_ID, VOCABULARY_NAME, VOCABULARY_REFERENCE, VOCABULARY_VERSION, VOCABULARY_CONCEPT_ID)
select 'CAPMC', 'CAPMC Identifier IDs', NULL, NULL, 2000000100



select * from OMOP_VOCABULARY.vocab5.VOCABULARY where VOCABULARY_ID like  'capm%'




--Concept_Class
insert into OMOP_VOCABULARY.vocab5.CONCEPT_CLASS (CONCEPT_CLASS_ID, CONCEPT_CLASS_NAME, CONCEPT_CLASS_CONCEPT_ID)
select 'System ID',	'System ID',	2000000200 


---Concept table entries
insert into OMOP_VOCABULARY.vocab5.CONCEPT (CONCEPT_ID, CONCEPT_NAME, DOMAIN_ID, VOCABULARY_ID, CONCEPT_CLASS_ID, 
		STANDARD_CONCEPT, CONCEPT_CODE, VALID_START_DATE, VALID_END_DATE, INVALID_REASON)
select 2000000000, 'CAPMC Identity Management', 'Identity Management', 'CAPMC', 'Domain',NULL,'CAPMC generated', '20140101', '20991231', NULL union all
