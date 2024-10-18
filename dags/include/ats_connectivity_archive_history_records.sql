--Step 4: Archive the Cur table snapshot
INSERT INTO lob_dq_replication_hist.ats_connectivity_hist_arch 
select 
    a.ministry_code,
    a.business_area,
    a.permit_type,
    a.project_id,
    a.application_id,
    a.project_name,
    a.project_description,
    a.project_location,
    a.received_date,
    a.adjudication_date,
    a.region_name,
    a.estimated_houses_connected,
    a.application_status,
    a.business_area_file_number,
    a.hash_key,
    a.record_active_ind,
    a.effective_start_dttm,
    a.effective_end_dttm,
    a.record_created_by,
    a.record_created_dttm,
    current_timestamp(0) archive_dttm,
    a.status_code,
    a.close_code,
    a.close_reason
  FROM	lob_dq_replication_hist.ats_connectivity_hist  a