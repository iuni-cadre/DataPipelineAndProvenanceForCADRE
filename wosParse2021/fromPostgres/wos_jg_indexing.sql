ALTER TABLE wos_jg_21.wos_jg ADD PRIMARY KEY (wosid); 

ALTER TABLE wos_jg_21.wos_jg ADD COLUMN title_tsv tsvector;
ALTER TABLE wos_jg_21.wos_jg ADD COLUMN abstract_tsv tsvector; 
ALTER TABLE wos_jg_21.wos_jg ADD COLUMN journal_tsv tsvector;

UPDATE wos_jg_21.wos_jg SET title_tsv = to_tsvector(papertitle);
UPDATE wos_jg_21.wos_jg SET abstract_tsv = to_tsvector(abstract);
UPDATE wos_jg_21.wos_jg SET journal_tsv = to_tsvector(journaltitle);

CREATE INDEX title_tsv_idx ON wos_jg_21.wos_jg USING GIN(title_tsv); 
CREATE INDEX abstract_tsv_idx ON wos_jg_21.wos_jg USING GIN(abstract_tsv); 
CREATE INDEX journal_tsv_idx ON wos_jg_21.wos_jg USING GIN(journal_tsv); 

--Trigram indexes: https://scoutapp.com/blog/how-to-make-text-searches-in-postgresql-faster-with-trigram-similarity

CREATE EXTENSION pg_trgm;

CREATE INDEX authors_full_name_idx ON wos_jg_21.wos_jg USING GIN (lc_standard_names wos_core.gin_trgm_ops);
CREATE INDEX index_on_address_trigram ON wos_jg_21.wos_jg USING GIN(full_address wos_core.gin_trgm_ops);
CREATE INDEX year_idx ON wos_jg_21.wos_jg USING btree (publicationyear);


CREATE INDEX citing_idx ON wos_jg_21.reference (citing); 
CREATE INDEX cited_idx ON wos_jg_21.reference (cited); 
