ALTER TABLE egf_bankaccount ADD COLUMN businessdetail CHARACTER VARYING(250)  NULL;
ALTER TABLE egf_bankaccount ADD UNIQUE (tenantId, businessdetail);
