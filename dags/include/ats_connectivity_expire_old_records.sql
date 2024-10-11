--Step1:Update end date and flag for changed records
Update lob_dq_replication_hist.ats_connectivity_hist a 
SET record_active_ind='E',effective_end_dttm=current_timestamp(0)- interval '1 minute'
from (
select 
		a.ministry_code					ministry_code,
		a.business_area 				business_area,
		a.permit_type		            permit_type,
		a.project_id 	                project_id,
		a.application_id		        application_id,
		a.project_name					project_name,
		a.project_description			project_description,
		a.project_location				project_location,
		a.received_date					received_date, 
		a.adjudication_date				adjudication_date, 
		a.region_name 					region_name,
		a.estimated_houses_connected	estimated_houses_connected,
		a.application_status			application_status,
	    a.business_area_file_number     business_area_file_number,
		md5(coalesce(cast(a.ministry_code					 		as varchar),'~') || '|'|| 
			coalesce(cast(a.business_area 				  	as varchar),'~') || '|'|| 
			coalesce(cast(a.permit_type		             		as varchar),'~') || '|'|| 
			coalesce(cast(a.project_id 	                  	as varchar),'~') || '|'|| 
			coalesce(cast(a.application_id		         		as varchar),'~') || '|'|| 
			coalesce(cast(a.project_name					 		as varchar),'~') || '|'|| 
			coalesce(cast(a.project_description			 		as varchar),'~') || '|'|| 
			coalesce(cast(a.project_location				 		as varchar),'~') || '|'|| 
			coalesce(cast(a.received_date					 		as varchar),'~') || '|'|| 
			coalesce(cast(a.adjudication_date				 		as varchar),'~') || '|'|| 
			coalesce(cast(a.region_name 					 		as varchar),'~') || '|'|| 
			coalesce(cast(a.estimated_houses_connected	    		 		as varchar),'~') || '|'|| 
			coalesce(cast(a.application_status			 		as varchar),'~') || '|'||
			coalesce(cast(a.business_area_file_number as varchar), '~') || '|' ||
        	coalesce(cast(a.status_code as varchar), '~') || '|' ||
       	 	coalesce(cast(a.close_code as varchar), '~') || '|' ||
       	 	coalesce(cast(a.close_reason as varchar), '~') 
			) as key_hash,
		'A' as record_active_ind,
		current_timestamp(0)			effective_start_dttm, 
		'9999-12-31 00:00:00'		effective_end_dttm,
		a.record_created_by record_created_by,
		a.record_created_dttm record_created_dttm,
		a.status_code,
		a.close_code,
		a.close_reason
FROM	lob_dq_replication.ats_connectivity a
left 	join  ( select * from lob_dq_replication_hist.ats_connectivity_hist where effective_end_dttm=CAST('9999-12-31 00:00:00' as timestamp(0)))d on 
        a.application_id = d.application_id and
		md5(coalesce(cast(a.ministry_code					 		as varchar),'~') || '|'|| 
			coalesce(cast(a.business_area 				  	as varchar),'~') || '|'|| 
			coalesce(cast(a.permit_type		             		as varchar),'~') || '|'|| 
			coalesce(cast(a.project_id 	                  	as varchar),'~') || '|'|| 
			coalesce(cast(a.application_id		         		as varchar),'~') || '|'|| 
			coalesce(cast(a.project_name					 		as varchar),'~') || '|'|| 
			coalesce(cast(a.project_description			 		as varchar),'~') || '|'|| 
			coalesce(cast(a.project_location				 		as varchar),'~') || '|'||  
			coalesce(cast(a.received_date					 		as varchar),'~') || '|'|| 
			coalesce(cast(a.adjudication_date				 		as varchar),'~') || '|'|| 
			coalesce(cast(a.region_name 					 		as varchar),'~') || '|'|| 
			coalesce(cast(a.estimated_houses_connected	    		 		as varchar),'~') || '|'|| 
			coalesce(cast(a.application_status			 		as varchar),'~') || '|'||
			coalesce(cast(a.business_area_file_number as varchar), '~') || '|' ||
        	coalesce(cast(a.status_code as varchar), '~') || '|' ||
       	 	coalesce(cast(a.close_code as varchar), '~') || '|' ||
       	 	coalesce(cast(a.close_reason as varchar), '~')
			)=d.hash_key 	 
 where d.hash_key is null and 
 a.application_id in (select application_id from lob_dq_replication_hist.ats_connectivity_hist where effective_end_dttm=CAST('9999-12-31 00:00:00' as timestamp(0)))
 )z
 
Where a.application_id=z.application_id and a.effective_end_dttm='9999-12-31 00:00:00';