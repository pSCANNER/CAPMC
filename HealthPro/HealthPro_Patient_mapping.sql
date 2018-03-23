/***********************************************************************
	--Author: Paulina Paul	
	--Create date: 03/16/2018
	--Project: CAPMC
	--Description: Maps the HealthPro patients to clarity patients
					using first name, last name, DOB, and (phone 
					or email or street address)
					Secondary match table maps patients based on first name 
					or last name. Manual review is needed before updating 
					the identifier table

***********************************************************************/

--HealthPro table
drop table #HealthPro 
select [PMI ID] as PMI_ID, [First Name] as First_name, [Last Name] as Last_name, [Date of Birth] as birth_date, email, phone,
replace(concat ([street Address], ' ', City, ' ', [state], ' ',  zip), ',', '') as [Address]
, [Physical Measurements Location], [Biospecimens Location]
into #HealthPro
 from pmi.healthpro.workqueue_CAL_PMC_20180319 HealthPro
where [Physical Measurements Location] in ('sdbbgateway', 'sandiegobb', 'uofcsandiego')
	or ([Physical Measurements Location] = '' and [Biospecimens Location] not in ('uofcirvine', 'usc'))
	or [Biospecimens Location] in ('sdbbgateway', 'sandiegobb', 'uofcsandiego')
	or ([Biospecimens Location] = '' and [Physical Measurements Location] not in ('uofcirvine', 'usc'))
 -- do not filter on withdrawn status until during/ after OMOP pull.


 
 select  [Physical Measurements Location], [Biospecimens Location], COUNT(*) from #healthpro group by  [Physical Measurements Location], [Biospecimens Location]



---------------------------------------------------------------------------------



--first match based on first, last name, DOB, phone, email and street address 
if OBJECT_ID('tempdb.dbo.#first_match') is not null drop table #first_match
select i.PMI_ID, p.master_id, i.first_name, i.last_name, p.first_name as Identity_first_name, p.last_name as Identity_last_name, 
i.birth_date, p.birth_date  as Identity_birth_date,
i.phone, p.[HomePhone], p.[WorkPhone], 
i.email, p.EMAIL_ADDRESS,
i.[address], p.[address] as identity_address,
1 as First_name_match,
1 as Last_name_match,
1 as DOB_match,
case when replace(replace(replace(REPLACE(i.phone, '(',''), ')', ''), ' ', ''), '-','') = 
			coalesce(replace(replace(replace(REPLACE(p.[HomePhone] , '(',''), ')', ''), ' ', ''), '-',''),
				 replace(replace(replace(REPLACE(p.[WorkPhone] , '(',''), ')', ''), ' ', ''), '-',''))
	  or replace(replace(replace(REPLACE(i.phone, '(',''), ')', ''), ' ', ''), '-','')
	  = coalesce(replace(replace(replace(REPLACE(p.[WorkPhone] , '(',''), ')', ''), ' ', ''), '-',''),
	   replace(replace(replace(REPLACE(p.[HomePhone] , '(',''), ')', ''), ' ', ''), '-',''))
	then 1 else 0 end as phone_match,
case when i.email = p.email_address	then 1 else 0 end as email_match,
case when i.[address] = p.[address] then 1 else 0 end as address_match,
0 as CountIdentifiersMatched
into #first_match
from #healthPro i 
full outer join (
		select  person.master_id, person.mrn, person.first_name, person.middle_name, person.surname as last_name, person.birth_date,
			homePhone.value as [HomePhone], workPhone.value as [WorkPhone], addr.value as [address], email.value as [email_address],
			iden.identifier as [Mapped_PMI_ID]
		from OMOP_V5_Pcornet_V3.omop5.phi_person person
		left join OMOP_V5_Pcornet_V3.omop5.phi_telecom homePhone on homePhone.master_id = person.master_id and homePhone.use_concept_id = 2000000400	-- home phone
		left join OMOP_V5_Pcornet_V3.omop5.phi_telecom workPhone on workPhone.master_id = person.master_id and workPhone.use_concept_id = 2000000401	-- work phone
		left join OMOP_V5_Pcornet_V3.omop5.phi_telecom email on email.master_id = person.master_id and email.use_concept_id = 2000000405			-- email 
		left join OMOP_V5_Pcornet_V3.omop5.phi_location addr on addr.master_id = person.master_id and addr.use_concept_id = 2000000511					---PostalAddressUse 
		left join OMOP_V5_Pcornet_V3.omop5.phi_identifier iden on iden.id_type_concept_id = 2000000813													--HealthPro Participant ID (PMI_ID)
		group by  person.master_id, person.mrn, person.first_name, person.middle_name, person.surname, person.birth_date,
			homePhone.value, workPhone.value, addr.value, email.value, iden.identifier 	
	) p
	on p.first_name = i.first_name 
	and p.last_name = i.last_name
	and p.birth_date = i.birth_date
where i.PMI_ID is not null 
and p.first_name is not null and p.last_name is not null and p.birth_date is not null 
and p.Mapped_PMI_ID is null --unmapped patients only
group by i.PMI_ID, p.master_id, i.first_name, i.last_name, p.first_name, p.last_name , 
i.birth_date, p.birth_date,
i.phone, p.[HomePhone], p.[WorkPhone], 
i.email, p.EMAIL_ADDRESS,
i.[address], p.[address]




--Count #matched identifiers 
update #first_match 
set CountIdentifiersMatched =  (First_name_match + Last_name_match + DOB_match + phone_match + email_match+ address_match)




--Check if 1 healthpro patient is mapped to multiple EHR patient (Manual review and/or phone call verification
--	 is needed if patient is matched to multiple EHR patients)
if OBJECT_ID('tempdb.dbo.#manual_match_review') is not null drop table #manual_match_review
select * 
into #manual_match_review 
from #first_match 
where pmi_id in (
	select PMI_ID
	from #first_match 
	group by PMI_ID 
	having COUNT(*) >1 
)

select * from #manual_match_review

---------------------------------------------------------------------------------

select * from #first_match where CountIdentifiersMatched < 4 
select * from #first_match where CountIdentifiersMatched >= 4 and pmi_id not in (select pmi_id from #manual_match_review)

---------------------------------------------------------------------------------

--update identity tables with the mapped PMI_ID (only if 4 or more identifiers matched)
declare @id_use_source_value_PMI varchar(100) = 'All of Us'
declare @id_type_source_value_PMI varchar(100) = 'PMI_ID' 
declare @system_PMI varchar(100) = 'HealthPro'
declare @assigner_PMI varchar(100) = 'HealthPro'
declare @concept_code_PMI varchar(100) = 'AoU'
declare @EHR_MRN_ID_Use bigint = (select concept_id from OMOP_VOCABULARY.vocab5.CONCEPT where CONCEPT_CODE = 'EHR_MRN_UCSD')  --Change to reflect each site.

INSERT OMOP_V5_Pcornet_V3.omop5.phi_identifier (preferred_record, master_id, source_id, id_use_concept_id
	, id_use_source_value, id_use_source_concept_id,
	identifier, id_type_concept_id, id_type_source_value, id_type_source_concept_id, [system], 
	period_start_date, period_end_date, assigner)  
SELECT	  0 as [preferred_record]
	, FM.master_id [master_id]
	, person.source_id as [source_id]
    , id_use.concept_id as [id_use_concept_id]
    , @id_use_source_value_PMI as [id_use_source_value]
    , id_use.concept_id as [id_use_source_concept_id]
    , FM.PMI_ID as [identifier]
    , id_type.concept_id as [id_type_concept_id]
    , @id_type_source_value_PMI as [id_type_source_value]
    , id_type.concept_id  as [id_type_source_concept_id]
    , @system_PMI as [system]
    , '1970-01-01' as [period_start_date]
    , '2099-12-31' as [period_end_date]
    , @assigner_PMI as [assigner]
FROM #first_match  FM
join OMOP_V5_Pcornet_V3.omop5.phi_identifier person on person.master_id = fm.master_id and person.id_use_concept_id = @EHR_MRN_ID_Use --UCSD EHR
left join omop_vocabulary.vocab5.concept id_use on id_use.vocabulary_id = 'CAPMC' and id_use.concept_class_id = 'Identifier Use'
		and id_use.concept_code = @concept_code_PMI and id_use.invalid_reason is NULL
left join omop_vocabulary.vocab5.concept id_type on id_type.vocabulary_id = 'CAPMC' and id_type.concept_class_id = 'Identifier Type'
		and id_type.concept_code = 'PMI_ID' and id_type.invalid_reason is NULL
left join #manual_match_review mmr on mmr.PMI_ID = FM.PMI_ID 
WHERE 
NOT EXISTS (SELECT identifier FROM OMOP_V5_Pcornet_V3.omop5.phi_identifier A2 
					WHERE A2.identifier = FM.PMI_ID
					and id_type_concept_id = 2000000813	--HealthPro Participant ID (PMI_ID)
					)			
and mmr.PMI_ID is null 	--exclude patients in the manual review table
AND FM.CountIdentifiersMatched >= 4 		
group by FM.master_id, FM.PMI_ID, person.source_id,id_use.concept_id, id_type.concept_id; 

---------------------------------------------------------------------------------

insert into #manual_match_review
select * from #first_match where CountIdentifiersMatched < 4  


---------------------------------------------------------------------------------


--Second match: (first or last name) and DOB and (email or phone or street address) 
-- for patients not matched in the previous step 
-- Manual review is needed for this section
if OBJECT_ID('tempdb.dbo.#second_match') is not null drop table #second_match
select i.PMI_ID, p.master_id, i.first_name, i.last_name, p.first_name as Identity_first_name, p.last_name as Identity_last_name, 
i.birth_date, p.birth_date  as Identity_birth_date,
i.phone, p.[HomePhone], p.[WorkPhone], 
i.email, p.EMAIL_ADDRESS,
i.[address], p.[address] as identity_address,
1 as First_name_match,
1 as Last_name_match,
1 as DOB_match,
case when replace(replace(replace(REPLACE(i.phone, '(',''), ')', ''), ' ', ''), '-','') = 
			coalesce(replace(replace(replace(REPLACE(p.[HomePhone] , '(',''), ')', ''), ' ', ''), '-',''),
				 replace(replace(replace(REPLACE(p.[WorkPhone] , '(',''), ')', ''), ' ', ''), '-',''))
	  or replace(replace(replace(REPLACE(i.phone, '(',''), ')', ''), ' ', ''), '-','')
	  = coalesce(replace(replace(replace(REPLACE(p.[WorkPhone] , '(',''), ')', ''), ' ', ''), '-',''),
	   replace(replace(replace(REPLACE(p.[HomePhone] , '(',''), ')', ''), ' ', ''), '-',''))
	then 1 else 0 end as phone_match,
case when i.email = p.email_address	then 1 else 0 end as email_match,
case when i.[address] = p.[address] then 1 else 0 end as address_match,
0 as CountIdentifiersMatched
--, p.Mapped_PMI_ID 
into #second_match
from #healthPro i 
full outer join (
		select  person.master_id, person.mrn, person.first_name, person.middle_name, person.surname as last_name, person.birth_date,
			homePhone.value as [HomePhone], workPhone.value as [WorkPhone], addr.value as [address], email.value as [email_address],
			iden.identifier as [Mapped_PMI_ID]
		from OMOP_V5_Pcornet_V3.omop5.phi_person person
		left join OMOP_V5_Pcornet_V3.omop5.phi_telecom homePhone on homePhone.master_id = person.master_id and homePhone.use_concept_id = 2000000400	-- home phone
		left join OMOP_V5_Pcornet_V3.omop5.phi_telecom workPhone on workPhone.master_id = person.master_id and workPhone.use_concept_id = 2000000401	-- work phone
		left join OMOP_V5_Pcornet_V3.omop5.phi_telecom email on email.master_id = person.master_id and email.use_concept_id = 2000000405			-- email 
		left join OMOP_V5_Pcornet_V3.omop5.phi_location addr on addr.master_id = person.master_id and addr.use_concept_id = 2000000511					---PostalAddressUse 
		left join OMOP_V5_Pcornet_V3.omop5.phi_identifier iden on iden.id_type_concept_id = 2000000813													--HealthPro Participant ID (PMI_ID)
		group by  person.master_id, person.mrn, person.first_name, person.middle_name, person.surname, person.birth_date,
			homePhone.value, workPhone.value, addr.value, email.value, iden.identifier 	
	) p
	on (p.first_name = i.first_name or p.last_name = i.last_name)
	and p.birth_date = i.birth_date
left join #manual_match_review mmr on mmr.PMI_ID = i.PMI_ID 
where i.PMI_ID is not null and i.PMI_ID not in (select PMI_ID from #first_match)
and (p.first_name is not null or p.last_name is not null) and p.birth_date is not null 
and p.Mapped_PMI_ID is null --unmapped patients only
and mmr.PMI_ID is null 	--exclude patients in the manual review table
group by i.PMI_ID, p.master_id, i.first_name, i.last_name, p.first_name, p.last_name, 
i.birth_date, p.birth_date, i.phone, p.[HomePhone], p.[WorkPhone], 
i.email, p.EMAIL_ADDRESS, i.[address], p.[address] ,p.Mapped_PMI_ID 
order by i.PMI_ID



-- Manual review is needed before updating identifiers
--Count #matched identifiers 
update #second_match 
set CountIdentifiersMatched =  (First_name_match + Last_name_match + DOB_match + phone_match + email_match+ address_match)


insert into #manual_match_review
select * from #second_match 

----------------------------------------------------------------------------------

--Manual matching and phone confirmation 
select * from #manual_match_review 
group by pmi_Id, master_id, first_name, last_name, 
identity_first_name, identity_last_name, birth_date, identity_birth_date,
phone, Homephone, workphone, email, email_address, address, identity_address,
first_name_match, last_name_match, DOB_match, phone_match, email_match, address_match,
CountIdentifiersMatched
order by PMI_ID 
----


