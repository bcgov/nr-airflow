INSERT INTO lob_dq_replication_hist.ats_housing_hist_arch
Select a.*,current_timestamp(0) from lob_dq_replication_hist.ats_housing_hist a