Update lob_dq_replication_hist.ats_housing_hist a 
SET record_active_ind='E',effective_end_dttm=current_timestamp(0)- interval '1 minute'
from (
select 
		a.ministry_code					ministry_code,
		a.business_area 				business_area,
		a.source_system_acronym 	    source_system_acronym,
		a.permit_type		            permit_type,
		a.project_id 	                project_id,
		a.application_id		        application_id,
		a.project_name					project_name,
		a.project_description			project_description,
		a.project_location				project_location,
		a.utm_easting		            utm_easting,
		a.utm_northing					utm_northing,
		a.received_date					received_date, 
		a.accepted_date		            accepted_date,
		a.adjudication_date				adjudication_date, 
		a.rejected_date		            rejected_date,
		a.amendment_renewal_date		amendment_renewal_date, 
		a.tech_review_completion_date	tech_review_completion_date,
		a.fn_consultn_start_date		fn_consultn_start_date, 
		a.fn_consultn_completion_date	fn_consultn_completion_date,
		a.fn_consultn_comment		    fn_consultn_comment,
		a.region_name 					region_name,
		a.indigenous_led_ind			indigenous_led_ind,
		a.rental_license_ind			rental_license_ind,
		a.social_housing_ind			social_housing_ind,
		a.housing_type					housing_type,    
		a.estimated_housing	    		estimated_housing,
		a.application_status			application_status,
	    a.business_area_file_number     business_area_file_number,
		md5(coalesce(cast(a.ministry_code					 		as varchar),'~') || '|'|| 
			coalesce(cast(a.business_area 				  	as varchar),'~') || '|'|| 
			coalesce(cast(a.source_system_acronym 	     		as varchar),'~') || '|'|| 
			coalesce(cast(a.permit_type		             		as varchar),'~') || '|'|| 
			coalesce(cast(a.project_id 	                  	as varchar),'~') || '|'|| 
			coalesce(cast(a.application_id		         		as varchar),'~') || '|'|| 
			coalesce(cast(a.project_name					 		as varchar),'~') || '|'|| 
			coalesce(cast(a.project_description			 		as varchar),'~') || '|'|| 
			coalesce(cast(a.project_location				 		as varchar),'~') || '|'|| 
			coalesce(cast(a.utm_easting		             		as varchar),'~') || '|'|| 
			coalesce(cast(a.utm_northing					 		as varchar),'~') || '|'|| 
			coalesce(cast(a.received_date					 		as varchar),'~') || '|'|| 
			coalesce(cast(a.accepted_date		             		as varchar),'~') || '|'|| 
			coalesce(cast(a.adjudication_date				 		as varchar),'~') || '|'|| 
			coalesce(cast(a.rejected_date		              	as varchar),'~') || '|'|| 
			coalesce(cast(a.amendment_renewal_date		 		as varchar),'~') || '|'|| 
			coalesce(cast(a.tech_review_completion_date	 		as varchar),'~') || '|'|| 
			coalesce(cast(a.fn_consultn_start_date		 		as varchar),'~') || '|'|| 
			coalesce(cast(a.fn_consultn_completion_date	 		as varchar),'~') || '|'|| 
			coalesce(cast(a.fn_consultn_comment		     		as varchar),'~') || '|'|| 
			coalesce(cast(a.region_name 					 		as varchar),'~') || '|'|| 
			coalesce(cast(a.indigenous_led_ind			  	as varchar),'~') || '|'|| 
			coalesce(cast(a.rental_license_ind			 		as varchar),'~') || '|'|| 
			coalesce(cast(a.social_housing_ind			 		as varchar),'~') || '|'|| 
			coalesce(cast(a.housing_type					 		as varchar),'~') || '|'|| 
			coalesce(cast(a.estimated_housing	    		 		as varchar),'~') || '|'|| 
			coalesce(cast(a.application_status			 		as varchar),'~') || '|'|| 
			coalesce(cast(a.business_area_file_number		 		as varchar),'~') || '|'||
            coalesce(cast(a.on_hold_reason_code					as varchar),'~') || '|'|| 
            coalesce(cast(a.on_hold_reason_description	 		as varchar),'~') || '|'|| 
            coalesce(cast(a.on_hold_start_date					as varchar),'~') || '|'|| 
            coalesce(cast(a.on_hold_end_date						as varchar),'~') || '|'|| 
            coalesce(cast(a.total_on_hold_time					as varchar),'~') || '|'|| 
            coalesce(cast(a.net_processing_time					as varchar),'~')        
			) as key_hash,
		'A' as record_active_ind,
		current_timestamp(0)			effective_start_dttm, 
		'9999-12-31 00:00:00'		effective_end_dttm,
		a.record_created_by record_created_by,
		a.record_created_dttm record_created_dttm,
		a.on_hold_reason_code,
		a.on_hold_reason_description,
		a.on_hold_start_date,
		a.on_hold_end_date,
		a.total_on_hold_time,
		a.net_processing_time			
FROM	lob_dq_replication.ats_housing  a
left 	join  ( select * from lob_dq_replication_hist.ats_housing_hist where effective_end_dttm=CAST('9999-12-31 00:00:00' as timestamp(0)))d on 
        a.application_id = d.application_id and
		md5(coalesce(cast(a.ministry_code					 		as varchar),'~') || '|'|| 
			coalesce(cast(a.business_area 				  	as varchar),'~') || '|'|| 
			coalesce(cast(a.source_system_acronym 	     		as varchar),'~') || '|'|| 
			coalesce(cast(a.permit_type		             		as varchar),'~') || '|'|| 
			coalesce(cast(a.project_id 	                  	as varchar),'~') || '|'|| 
			coalesce(cast(a.application_id		         		as varchar),'~') || '|'|| 
			coalesce(cast(a.project_name					 		as varchar),'~') || '|'|| 
			coalesce(cast(a.project_description			 		as varchar),'~') || '|'|| 
			coalesce(cast(a.project_location				 		as varchar),'~') || '|'|| 
			coalesce(cast(a.utm_easting		             		as varchar),'~') || '|'|| 
			coalesce(cast(a.utm_northing					 		as varchar),'~') || '|'|| 
			coalesce(cast(a.received_date					 		as varchar),'~') || '|'|| 
			coalesce(cast(a.accepted_date		             		as varchar),'~') || '|'|| 
			coalesce(cast(a.adjudication_date				 		as varchar),'~') || '|'|| 
			coalesce(cast(a.rejected_date		              	as varchar),'~') || '|'|| 
			coalesce(cast(a.amendment_renewal_date		 		as varchar),'~') || '|'|| 
			coalesce(cast(a.tech_review_completion_date	 		as varchar),'~') || '|'|| 
			coalesce(cast(a.fn_consultn_start_date		 		as varchar),'~') || '|'|| 
			coalesce(cast(a.fn_consultn_completion_date	 		as varchar),'~') || '|'|| 
			coalesce(cast(a.fn_consultn_comment		     		as varchar),'~') || '|'|| 
			coalesce(cast(a.region_name 					 		as varchar),'~') || '|'|| 
			coalesce(cast(a.indigenous_led_ind			  	as varchar),'~') || '|'|| 
			coalesce(cast(a.rental_license_ind			 		as varchar),'~') || '|'|| 
			coalesce(cast(a.social_housing_ind			 		as varchar),'~') || '|'|| 
			coalesce(cast(a.housing_type					 		as varchar),'~') || '|'|| 
			coalesce(cast(a.estimated_housing	    		 		as varchar),'~') || '|'|| 
			coalesce(cast(a.application_status			 		as varchar),'~') || '|'|| 
			coalesce(cast(a.business_area_file_number		 		as varchar),'~') || '|'||
            coalesce(cast(a.on_hold_reason_code					as varchar),'~') || '|'|| 
            coalesce(cast(a.on_hold_reason_description	 		as varchar),'~') || '|'|| 
            coalesce(cast(a.on_hold_start_date					as varchar),'~') || '|'|| 
            coalesce(cast(a.on_hold_end_date						as varchar),'~') || '|'||
            coalesce(cast(a.total_on_hold_time					as varchar),'~') || '|'|| 
            coalesce(cast(a.net_processing_time					as varchar),'~')  
			)=d.hash_key 	 
 where d.hash_key is null and 
 a.application_id in (select application_id from lob_dq_replication_hist.ats_housing_hist where effective_end_dttm=CAST('9999-12-31 00:00:00' as timestamp(0)))
 )z
 
Where a.application_id=z.application_id and a.effective_end_dttm='9999-12-31 00:00:00';