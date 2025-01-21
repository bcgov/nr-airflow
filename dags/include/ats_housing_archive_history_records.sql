INSERT INTO lob_dq_replication_hist.ats_housing_hist_arch
Select a.ministry_code,
    a.business_area,
    a.source_system_acronym,
    a.permit_type,
    a.project_id,
    a.application_id,
    a.project_name,
    a.project_description,
    a.project_location,
    a.utm_easting,
    a.utm_northing,
    a.received_date,
    a.accepted_date,
    a.adjudication_date,
    a.rejected_date,
    a.amendment_renewal_date,
    a.tech_review_completion_date,
    a.fn_consultn_start_date,
    a.fn_consultn_completion_date,
    a.fn_consultn_comment,
    a.region_name,
    a.indigenous_led_ind,
    a.rental_license_ind,
    a.social_housing_ind,
    a.housing_type,
    a.estimated_housing,
    a.application_status,
    a.business_area_file_number,
    a.hash_key,
    a.record_active_ind,
    a.effective_start_dttm,
    a.effective_end_dttm,
    a.record_created_by,
    a.record_created_dttm,
    current_timestamp(0) archive_dttm,
    a.on_hold_reason_code,
	a.on_hold_reason_description,
	a.on_hold_start_date,
	a.on_hold_end_date,
	a.total_on_hold_time,
	a.net_processing_time
    from lob_dq_replication_hist.ats_housing_hist a