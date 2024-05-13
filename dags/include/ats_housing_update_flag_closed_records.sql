Update lob_dq_replication_hist.ats_housing_hist a 
SET record_active_ind='C'
from (
select d.application_id application_id
FROM ( select * from lob_dq_replication_hist.ats_housing_hist where effective_end_dttm=CAST('9999-12-31 00:00:00' as timestamp(0)))d 
	left 	join  lob_dq_replication.ats_housing  a on 
        a.application_id = d.application_id 
 where a.application_id is null
 )z 
Where a.application_id=z.application_id and a.effective_end_dttm='9999-12-31 00:00:00';