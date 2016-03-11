A = LOAD 'ehitrans_dev.stg_ipaas_claim' using org.apache.hcatalog.pig.HCatLoader();

B = FILTER A BY ((claim_adj_cd IS NULL) OR (claim_adj_cd == 'null') OR (claim_adj_cd == ''));

C = FILTER A BY ((claim_adj_cd IS NOT NULL) AND (claim_adj_cd != 'null') AND (claim_adj_cd !=''));

D = FOREACH B GENERATE rec_id..claim_nbr, '0' AS claim_adj_cd, adj_from_claim_nbr..submit_drg_code_tp_cd;

RESULT = UNION C, D;
rmf '/workspace/EHITRANS_DEV/STG_IPAAS_CLAIM/*';

STORE RESULT INTO '/workspace/EHITRANS_DEV/tmp/STG_IPAAS_CLAIM' using PigStorage('\t');
rmf /workspace/EHITRANS_DEV/STG_IPAAS_CLAIM;
mv /workspace/EHITRANS_DEV/tmp/STG_IPAAS_CLAIM /workspace/EHITRANS_DEV;
