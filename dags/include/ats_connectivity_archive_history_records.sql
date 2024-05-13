--Step 4: Archive the Cur table snapshot
INSERT INTO lob_dq_replication_hist.ats_connectivity_hist_arch
Select a.*,current_timestamp(0) from lob_dq_replication_hist.ats_connectivity_hist a