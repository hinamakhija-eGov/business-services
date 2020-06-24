ALTER TABLE egf_bankaccount ADD COLUMN businessservice CHARACTER VARYING(250)  NULL;
ALTER TABLE egf_bankaccount ADD UNIQUE (tenantId, businessservice);
