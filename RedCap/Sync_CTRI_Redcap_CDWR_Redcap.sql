use PMI
go

----Step 1: Load CTRI's Redcap data into CDWR Redcap Staging table (replaced with the SSIS package E:\CAPMC\Redcap\Redcap_Sync_Exports\Redcap_import_051418)
--truncate table pmi.redcap.redcap_staging 

--BULK INSERT PMI.redcap.redcap_staging FROM 'F:\CAPMC\RedCap\Redcap_Exports\Redcap_export.csv' 
--WITH (
--FIRSTROW = 2,
--FIELDTERMINATOR = ',',
--ROWTERMINATOR = '0x0a'
--)
--go 



----replace double quotes from the CSV import 
--exec pmi.redcap.replace_dbl_quotes




--Step 2: reset sequence for the redcap Study ID
declare @stmt nvarchar(255) = 'alter sequence redcap.study_id_sequence restart with ' 
									+ cast((select  MAX(CAST(study_id as int))+1 from PMI.redcap.redcap_staging) as nvarchar(20))
										+ ' increment by 1'
exec sp_executesql  @stmt 





--Step 2: identify duplicate patients (4/5 identfiers should match)
--Remove Duplicate patient records
if OBJECT_ID('tempdb.dbo.#dup') is not null drop table #dup 
		select distinct rs1.* 
		into #dup 
		from pmi.redcap.redcap_staging rs1	
		join (
				select first_name, last_name, dob, phone_number, email_address from pmi.redcap.redcap_staging 
				group by first_name, last_name, dob, phone_number, email_address having COUNT(*) >1
				union 
				select first_name, last_name, dob, phone_number, email_address 	 from PMI.redcap.redcap_staging
				where duplicate_record___yes = 1 
				) rs2
				on (rs2.first_name = rs1.first_name and rs2.last_name = rs1.last_name and rs2.dob = rs1.dob) 
		where rs1.first_name is not null and rs1.last_name is not null 
order by rs1.first_name, rs1.last_name, rs1.dob


select * from #dup order by first_name 

select * from pmi.Redcap.redcap_staging where pmi_id in (select pmi_id from pmi.Redcap.redcap_staging group by pmi_id having count(*) >1 ) order by pmi_id
select * from pmi.Redcap.redcap_staging where MRN in (select MRN from pmi.Redcap.redcap_staging group by MRN having count(*) >1 ) order by mrn



--Step 3: Resolving duplicate patients (reviewing and deletion is manual effort)

--Step 4: Merging Redcap-Master with Redcap-Staging
merge pmi.redcap.redcap_master as target
using (select Study_ID
		,duplicate_record___yes
		,MRN
		,First_name
		,last_name
		,DOB
		,street_address_line_1
		,street_address_line_2
		,city
		,[state]
		,zip_code
		,email_address
		,phone_number
		,pmi_id
		,withdrawn___yes
		,withdrawn_date
		,withdrawn_recorded_in_healthpro
		 from pmi.redcap.redcap_staging) as source
on source.study_id = target.study_id 
when matched then
	update set 
		target.MRN = source.MRN
		,target.duplicate_record___yes = source.duplicate_record___yes
		, target.pmi_id = source.pmi_id
		, target.withdrawn___yes = source.withdrawn___yes
		, target.withdrawn_date = source.withdrawn_date
		, target.withdrawn_recorded_in_healthpro = source.withdrawn_recorded_in_healthpro
		, target.[update_date] = getdate()
		, target.[source] = 'Sync CTRI Redcap'
when not matched by target then 
	insert (Study_ID
		,duplicate_record___yes
		,MRN
		,First_name
		,last_name
		,DOB
		,street_address_line_1
		,street_address_line_2
		,city
		,[state]
		,zip_code
		,email_address
		,phone_number
		,pmi_id
		,withdrawn___yes
		,withdrawn_date
		,withdrawn_recorded_in_healthpro
		,update_date
		,[source]
		)
	values (source.Study_ID
		,source.duplicate_record___yes
		,source.MRN
		,source.First_name
		,source.last_name
		,source.DOB
		,source.street_address_line_1
		,source.street_address_line_2
		,source.city
		,source.[state]
		,source.zip_code
		,source.email_address
		,source.phone_number
		,source.pmi_id
		,source.withdrawn___yes
		,source.withdrawn_date
		,source.withdrawn_recorded_in_healthpro
		,getdate()
		,'Sync CTRI Redcap'
		)
when not matched by source then delete
	--update set target.deleted_in_CTRI_Redcap = '1'  --flags records that are not present in CTRI Redcap
OUTPUT $action, deleted.study_id as Deleted_StudyID, deleted.MRN as Deleted_MRN, deleted.pmi_id as Deleted_PMI_ID,
	inserted.study_id as Inserted_studyID, inserted.MRN as Inserted_MRN, inserted.pmi_id as Inserted_PMI_ID
;





					----------------
					--testing
					select COUNT(*) from pmi.redcap.redcap_staging
					select * from pmi.redcap.redcap_master where deleted_in_CTRI_Redcap = '1'

					select * from PMI.redcap.redcap_staging where study_id not in (select study_id from PMI.redcap.redcap_master)

					select rm.MRN, rm.first_name, rm.last_name, rm.dob ,
					src.MRN, src.pat_first_name, src.pat_last_name, src.birth_date
					from pmi.Redcap.redcap_master rm
					left join [CDWRMAP-ENCRYPT].dbo.patient_src src on src.MRN = '0'+rm.mrn
					where len(rm.MRN) <8

					update pmi.Redcap.redcap_master
					set MRN = '0'+ mrn
					where len(MRN) <8



					----------------
					
-- Match patients with no MRN 
if OBJECT_ID ('tempdb.dbo.#NoMRN') is not null drop table #NoMRN 
select rm.study_id, rm.mrn, rm.first_name, rm.last_name, rm.dob, rm.phone_number, rm.email_address,
person.master_id, MRN.identifier as PHI_MRN, MRN.source_id,
 person.first_name as Person_first_name, person.surname, person.birth_date, 
hphone.value as homePhone, wphone.value as workPhone, email.value as Email,
case when rm.dob = person.birth_date then 1 else 0 end as DOB_match,
case when replace(replace(replace(REPLACE(rm.phone_number, '(',''), ')', ''), ' ', ''), '-','') = 
			coalesce(replace(replace(replace(REPLACE(hphone.value , '(',''), ')', ''), ' ', ''), '-',''),
				 replace(replace(replace(REPLACE(wphone.value , '(',''), ')', ''), ' ', ''), '-',''))
	  or replace(replace(replace(REPLACE(rm.phone_number, '(',''), ')', ''), ' ', ''), '-','')
	  = coalesce(replace(replace(replace(REPLACE(wphone.value , '(',''), ')', ''), ' ', ''), '-',''),
	   replace(replace(replace(REPLACE(hphone.value , '(',''), ')', ''), ' ', ''), '-',''))
	then 1 else 0 end as phone_match,
case when rm.email_address = email.value then 1 else 0 end as Email_match,
2 as TotalIdentifiersMatched
into #NoMRn
 from pmi.redcap.redcap_master rm
left join OMOP_V5_Pcornet_V3.omop5.phi_person person on person.first_name = rm.first_name and person.surname = rm.last_name 
LEFT join OMOP_V5_Pcornet_V3.omop5.phi_identifier MRN on MRN.master_id = person.master_id and MRN.id_type_concept_id = 2000000803 --MRN 
left join omop_v5_pcornet_v3.omop5.phi_telecom hphone on hphone.master_id = person.master_id and  hphone.value = rm.phone_number and hphone.use_concept_id  = 2000000400  --home phone
left join omop_v5_pcornet_v3.omop5.phi_telecom wphone on wphone.master_id = person.master_id and wphone.value = rm.phone_number and wphone.use_concept_id  = 2000000401  --work phone
left join omop_v5_pcornet_v3.omop5.phi_telecom email on email.master_id = person.master_id and email.value = rm.email_address and email.use_concept_id  = 2000000405  --Email

 where rm.mrn is null and rm.first_name is not null 
 group by rm.study_id, rm.mrn, rm.first_name, rm.last_name, rm.dob, rm.phone_number, rm.email_address,
 person.master_id, MRN.identifier , MRN.source_id,
person.first_name, person.surname, person.birth_date,
hphone.value, wphone.value, email.value
 
 --update the match count
 update #NoMRn
 set TotalIdentifiersMatched = TotalIdentifiersMatched + dob_match + phone_match + email_match 
 
--give priority to max # of matches
if OBJECT_ID ('tempdb.dbo.#check') is not null drop table #check 
select * , ROW_NUMBER() over (partition by study_id order by TotalIdentifiersMatched desc) as rownum
into #check 
from #NoMRN 
where TotalIdentifiersMatched >= 4

--select records matches for each person based on the highest #matches.
select * from #check where rownum = 1

--update the mapped MRNs in redcap_master table
update rm
set rm.MRN = nm.PHI_MRN
from PMI.redcap.redcap_master rm
join #check nm on nm.study_id = rm.study_id 
where rm.mrn is null and rm.first_name is not null 




--Step 5: Update Redcap Study_ID's in the PHI - identifier tables
if OBJECT_ID ('tempdb.dbo.#redcapMatching') is not null drop table #redcapMatching 
select rm.study_id, rm.mrn, 
		rm.first_name, rm.last_name, rm.dob, rm.phone_number, rm.email_address,
		phi_MRN.master_id as PHI_MRN_MasterID ,phi_mrn.identifier as PHI_MRN, phi_MRN.source_id as phi_MRN_SourceID,
		MRN_person.first_name as MRN_firstname, MRN_person.surname MRN_surname, MRN_person.birth_date MRN_Birthdate, 
		hphone.value AS MRN_hphone, wphone.value AS MRN_wphone, email.value AS MRN_email
		--phi_studyID.master_id as PHI_STUDYID_Masterid, PHI_studyID.identifier as PHI_StudyID, PHI_studyID.source_id as PHI_studyID_SourceID, StudyID_MRN.identifier as StudyID_MRN
		--STUDYID_person.first_name STUDYID_FirstName, STUDYID_person.surname STUDYID_SurName, STUDYID_person.birth_date STUDYID_birthdate, 
		--STUDYID_hphone.value AS studyID_hphone, STUDYID_wphone.value AS studyID_wphone, STUDYID_email.value AS studyID_email

into #redcapMatching
from pmi.redcap.redcap_master rm 
--left join OMOP_V5_Pcornet_V3.omop5.phi_identifier PHI_StudyID on PHI_StudyID.identifier = rm.study_id and phi_StudyID.id_type_concept_id = 2000000814 --Redcap studyID
--left join OMOP_V5_Pcornet_V3.omop5.phi_identifier StudyID_MRN on (StudyID_MRN.master_id = PHI_StudyID.master_id) and StudyID_MRN.id_type_concept_id = 2000000803 --MRN 

left join OMOP_V5_Pcornet_V3.omop5.phi_identifier phi_MRN
	on (rm.mrn = phi_MRN.identifier or ('0'+rm.mrn) = phi_MRN.identifier) and phi_MRN.id_type_concept_id = 2000000803 --MRN 
	
--left join OMOP_V5_Pcornet_V3.omop5.phi_person STUDYID_person on STUDYID_person.master_id = PHI_StudyID.master_id
--left join OMOP_V5_Pcornet_V3.omop5.phi_telecom STUDYID_hphone on STUDYID_hphone.master_id = PHI_StudyID.master_id and STUDYID_hphone.use_concept_id = 2000000400  --home phone
--left join OMOP_V5_Pcornet_V3.omop5.phi_telecom STUDYID_wphone on STUDYID_wphone.master_id = PHI_StudyID.master_id and STUDYID_wphone.use_concept_id = 2000000401  --work phone
--left join OMOP_V5_Pcornet_V3.omop5.phi_telecom STUDYID_email on STUDYID_email.master_id = PHI_StudyID.master_id and STUDYID_email.use_concept_id = 2000000405  --EMail

left join OMOP_V5_Pcornet_V3.omop5.phi_person MRN_person on MRN_person.master_id = phi_MRN.master_id
left join OMOP_V5_Pcornet_V3.omop5.phi_telecom hphone on hphone.master_id = phi_MRN.master_id and hphone.use_concept_id = 2000000400  --home phone
left join OMOP_V5_Pcornet_V3.omop5.phi_telecom wphone on wphone.master_id = phi_MRN.master_id and wphone.use_concept_id = 2000000401  --work phone
left join OMOP_V5_Pcornet_V3.omop5.phi_telecom email on email.master_id = phi_MRN.master_id and email.use_concept_id = 2000000405  --EMail

where rm.first_name is not null and rm.last_name is not null 

group by rm.study_id, rm.mrn, --phi_studyID.master_id , PHI_studyID.identifier , StudyID_MRN.identifier ,
		phi_MRN.master_id  ,phi_mrn.identifier, phi_MRN.source_id ,
		rm.first_name, rm.last_name, rm.dob, rm.phone_number, rm.email_address,
		--STUDYID_person.first_name , STUDYID_person.surname , STUDYID_person.birth_date ,  PHI_studyID.source_id ,
		--STUDYID_hphone.value , STUDYID_wphone.value , STUDYID_email.value , 
		MRN_person.first_name, MRN_person.surname, MRN_person.birth_date, hphone.value , wphone.value , email.value





select COUNT(*) from #redcapMatching where  PHI_MRN_MasterID is not null 

select COUNT(*) from #redcapMatching where  mrn  is not null 
select COUNT(*) from #redcapMatching
select * from #redcapMatching where MRN is null 

select COUNT(*) from PMI.redcap.redcap_master where mrn is not null 
select COUNT(*) from PMI.redcap.redcap_master 

select * from #redcapMatching where  PHI_MRN_MasterID is null and MRN is not null 


select master_id from #redcap group by master_id having COUNT(*) >1
select MRN from #redcap where MRN is not null  group by MRN having COUNT(*) >1

			-----
			----one time insert of all redcap studyIDs 

			--select *
			--from #redcapMatching rm

			-- --Redcap Study_ID: Change values for the following corresponding to each site.
			--declare @id_use_source_value_redcap varchar(100) = 'CAPMC Redcap Study ID'
			--declare @id_type_source_value_redcap varchar(100) = 'Redcap Study ID' 
			--declare @system_redcap varchar(100) = 'UCSD Redcap'
			--declare @assigner_redcap varchar(100) = 'UCSD Redcap'
			--declare @concept_code_redcap varchar(100) = 'REDCAP_STUDYID_UCSD'

			--insert into OMOP_V5_Pcornet_V3.omop5.phi_identifier (
			--	-- phi_identifier_id
			--		preferred_record
			--		, master_id
			--		, source_id
			--		, id_use_concept_id
			--		, id_use_source_value
			--		, id_use_source_concept_id
			--		, identifier
			--		, id_type_concept_id
			--		, id_type_source_value
			--		, id_type_source_concept_id
			--		, [system]
			--		, period_start_date
			--		, period_end_date
			--		, assigner
			--		)
			--select 
			--	  0 as [preferred_record]
			--	, phi.PHI_MRN_masterID [master_id]
			--	, phi.PHI_MRN_sourceID as [source_id]
			--    , id_use.concept_id as [id_use_concept_id]
			--    , @id_use_source_value_redcap as [id_use_source_value]
			--    , id_use.concept_id as [id_use_source_concept_id]
			--    , phi.Study_ID as [identifier]
			--    , id_type.concept_id as [id_type_concept_id]
			--    , @id_type_source_value_redcap as [id_type_source_value]
			--    , id_type.concept_id  as [id_type_source_concept_id]
			--    , @system_redcap as [system]
			--    , '1970-01-01' as [period_start_date]
			--    , '2099-12-31' as [period_end_date]
			--    , @assigner_redcap as [assigner]
			--from  #redcapMatching phi
			--left join omop_vocabulary.vocab5.concept id_use on id_use.vocabulary_id = 'CAPMC' and id_use.concept_class_id = 'Identifier Use'
			--		and id_use.concept_code = @concept_code_redcap and id_use.invalid_reason is NULL
			--left join omop_vocabulary.vocab5.concept id_type on id_type.vocabulary_id = 'CAPMC' and id_type.concept_class_id = 'Identifier Type'
			--		and id_type.concept_code = 'STUDY_ID' and id_type.invalid_reason is NULL
			--  group by  phi.PHI_MRN_masterID, phi.PHI_MRN_sourceID  , id_use.CONCEPT_ID, id_type.CONCEPT_ID, phi.Study_ID


---

--update PHI_Identifier table with Redcap StudyID

	merge OMOP_V5_Pcornet_V3.omop5.phi_identifier as target
	using (
			select * from #redcapMatching rm 
			--where PHI_MRN_masterID = PHI_STUDYID_masterid --and PHI_MRN_masterid is not null --and PHI_studyID_masterID is not null 
		) source on target.identifier = source.study_id and target.id_type_concept_id = 2000000814 --Redcap studyID
	--when matched and the studyID is mapped to masterID/MRN => Do nothing
	when matched and target.master_id is null then 
			update set 
				target.master_id = case when source.PHI_MRN_masterID is not null then source.PHI_MRN_masterID 
										--when source.phi_studyID_masterID is not null then source.phi_studyID_masterID 
										else null end  
				, target.source_id = case when source.PHI_MRN_masterID is not null then source.PHI_MRN_sourceid
										--when source.phi_studyID_masterID is not null then source.phi_studyID_sourceid
										else null end 
	when not matched by target then 
			--insert new records
			insert (preferred_record, master_id, source_id, id_use_concept_id, id_use_source_value, id_use_source_concept_id, identifier, 
					id_type_concept_id, id_type_source_value,  id_type_source_concept_id, [system], period_start_date, period_end_date, assigner
				)
			values (
				0  
				, case when source.PHI_MRN_masterID is not null then source.PHI_MRN_masterID 
					--	when source.phi_studyID_masterID is not null then source.phi_studyID_masterID 
						else null end  
				, case when source.PHI_MRN_masterID is not null then source.PHI_MRN_sourceid
					--	when source.phi_studyID_masterID is not null then source.phi_studyID_sourceid
						else null end  
				, 2000000720	
				, 'CAPMC Redcap Study ID'
				, 2000000720
				, source.study_id  
				, 2000000814	
				, 'Redcap Study ID'	
				, 2000000814	
				, 'UCSD Redcap'
				, '1970-01-01'
				, '2099-12-31'
				, 'UCSD Redcap'
				)
	--when not matched by source then delete 
			--delete records not in the CTRI redcap system
	;	
			
			--TESTING 
			select top 19 * from OMOP_V5_Pcornet_V3.omop5.phi_identifier where id_type_concept_id = 2000000814 --Redcap studyID
			and identifier in ('206210','206212')
			
			SELECT * FROM #REDCAPMATCHING WHERE STUDY_ID in ('206210','206212')
			
			SELECT RM.*
			FROM PMI.redcap.redcap_master RM 
			LEFT JOIN OMOP_V5_Pcornet_V3.omop5.phi_identifier STID ON STID.identifier = RM.study_id AND STID.id_type_concept_id = 2000000814 --Redcap studyID
			WHERE STID.identifier IS NULL 