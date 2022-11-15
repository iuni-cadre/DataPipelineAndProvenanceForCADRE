CREATE TABLE IF NOT EXISTS patview_core.g_applicant_not_disambiguated (
	patent_id VARCHAR(36),
	applicant_sequence INTEGER,
	raw_applicant_name_first VARCHAR(64),
	raw_applicant_name_last VARCHAR(64),
	raw_applicant_organization VARCHAR(256),
	applicant_type VARCHAR(30),
	applicant_designation VARCHAR(20),
	rawlocation_id VARCHAR(36)
);

CREATE TABLE IF NOT EXISTS patview_core.g_application (
	application_id VARCHAR(36),
	patent_id VARCHAR(20),
	patent_application_type VARCHAR(20),
	filing_date VARCHAR(10),
	series_code VARCHAR(2),
	rule_47_flag BOOLEAN
);

CREATE TABLE IF NOT EXISTS patview_core.g_assignee_disambiguated (
	patent_id VARCHAR(20),
	assignee_sequence INTEGER,
	assignee_id VARCHAR(36),
	disambig_assignee_individual_name_first VARCHAR(96),
	disambig_assignee_individual_name_last VARCHAR(96),
	disambig_assignee_organization VARCHAR(256),
	assignee_type INTEGER,
	location_id VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS patview_core.g_assignee_not_disambiguated (
	patent_id VARCHAR(20),
	assignee_sequence INTEGER,
	assignee_id VARCHAR(36),
	raw_assignee_individual_name_first VARCHAR(64),
	raw_assignee_individual_name_last VARCHAR(64),
	raw_assignee_organization VARCHAR(256),
	assignee_type INTEGER,
	rawlocation_id VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS patview_core.g_attorney_disambiguated (
	patent_id VARCHAR(20),
	attorney_sequence INTEGER,
	attorney_id VARCHAR(36),
	disambig_attorney_name_first VARCHAR(64),
	disambig_attorney_name_last VARCHAR(64),
	disambig_attorney_organization VARCHAR(128),
	attorney_country VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS patview_core.g_attorney_not_disambiguated (
	patent_id VARCHAR(20),
	attorney_sequence INTEGER,
	attorney_id VARCHAR(36),
	raw_attorney_name_first VARCHAR(64),
	raw_attorney_name_last VARCHAR(64),
	raw_attorney_organization VARCHAR(128),
	attorney_country VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS patview_core.g_botanic (
	patent_id VARCHAR(20),
	latin_name VARCHAR(128),
	plant_variety VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS patview_core.g_brf_sum_text (
	patent_id VARCHAR(20),
	summary_text TEXT
);

CREATE TABLE IF NOT EXISTS patview_core.g_claim (
	patent_id VARCHAR(20),
	claim_sequence INTEGER,
	claim_text TEXT,
	dependent VARCHAR(512),
	claim_number VARCHAR(10),
	exemplary INTEGER
);

CREATE TABLE IF NOT EXISTS patview_core.g_cpc_current (
	patent_id VARCHAR(20),
	cpc_sequence INTEGER,
	cpc_section VARCHAR(10),
	cpc_class VARCHAR(20),
	cpc_subclass VARCHAR(20),
	cpc_group VARCHAR(32),
	cpc_type VARCHAR(36),
	cpc_symbol_position VARCHAR(2)
);

CREATE TABLE IF NOT EXISTS patview_core.g_cpc_title (
	cpc_subclass VARCHAR(20),
	cpc_subclass_title VARCHAR(512),
	cpc_group VARCHAR(20),
	cpc_group_title TEXT,
	cpc_class VARCHAR(20),
	cpc_class_title VARCHAR(512)
);

CREATE TABLE IF NOT EXISTS patview_core.g_detail_desc_text (
	patent_id VARCHAR(20),
	description_text TEXT,
	description_length INTEGER
);

CREATE TABLE IF NOT EXISTS patview_core.g_draw_desc_text (
	patent_id VARCHAR(20),
	draw_desc_sequence INTEGER,
	draw_desc_text TEXT
);

CREATE TABLE IF NOT EXISTS patview_core.g_examiner_not_disambiguated (
	patent_id VARCHAR(20),
	examiner_sequence INTEGER,
	raw_examiner_name_first VARCHAR(64),
	raw_examiner_name_last VARCHAR(64),
	examiner_role VARCHAR(20),
	art_group VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS patview_core.g_figures (
	patent_id VARCHAR(20),
	num_figures INTEGER,
	num_sheets INTEGER
);

CREATE TABLE IF NOT EXISTS patview_core.g_foreign_citation (
	patent_id VARCHAR(20),
	citation_sequence INTEGER,
	citation_application_id VARCHAR(64),
	citation_date VARCHAR(10),
	citation_category VARCHAR(64),
	citation_country VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS patview_core.g_foreign_priority (
	patent_id VARCHAR(20),
	priority_claim_sequence INTEGER,
	priority_claim_kind VARCHAR(20),
	foreign_application_id VARCHAR(70),
	filing_date VARCHAR(10),
	foreign_country_filed VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS patview_core.g_gov_interest (
	patent_id VARCHAR(20),
	gi_statement TEXT
);

CREATE TABLE IF NOT EXISTS patview_core.g_gov_interest_contracts (
	patent_id VARCHAR(20),
	contract_award_number VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS patview_core.g_gov_interest_org (
	patent_id VARCHAR(255),
	gi_organization_id INTEGER,
	fedagency_name VARCHAR(255),
	level_one VARCHAR(255),
	level_two VARCHAR(255),
	level_three VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS patview_core.g_inventor_disambiguated (
	patent_id VARCHAR(36),
	inventor_sequence INTEGER,
	inventor_id VARCHAR(128),
	disambig_inventor_name_first TEXT,
	disambig_inventor_name_last TEXT,
	male_flag INTEGER,
	attribution_status INTEGER
);

CREATE TABLE IF NOT EXISTS patview_core.g_inventor_not_disambiguated (
	patent_id VARCHAR(20),
	inventor_sequence INTEGER,
	inventor_id VARCHAR(80),
	raw_inventor_name_first VARCHAR(64),
	raw_inventor_name_last VARCHAR(64),
	deceased_flag BOOLEAN,
	rawlocation_id VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS patview_core.g_ipc_at_issue (
	patent_id VARCHAR(20),
	ipc_sequence INTEGER,
	classification_level VARCHAR(20),
	section VARCHAR(20),
	ipc_class VARCHAR(20),
	subclass VARCHAR(20),
	main_group VARCHAR(20),
	subgroup VARCHAR(20),
	symbol_position VARCHAR(20),
	classification_value VARCHAR(20),
	classification_status VARCHAR(20),
	classification_data_source VARCHAR(20),
	action_date VARCHAR(10),
	ipc_version_indicator VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS patview_core.g_location_disambiguated (
	location_id VARCHAR(128),
	disambig_city VARCHAR(128),
	disambig_state VARCHAR(20),
	disambig_country VARCHAR(16),
	latitude FLOAT,
	longitude FLOAT,
	county VARCHAR(60),
	state_fips VARCHAR(2),
	county_fips VARCHAR(6)
);

CREATE TABLE IF NOT EXISTS patview_core.g_location_not_disambiguated (
	rawlocation_id VARCHAR(128),
	location_id VARCHAR(128),
	raw_city VARCHAR(128),
	raw_state VARCHAR(20),
	raw_country VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS patview_core.g_other_reference (
	patent_id VARCHAR(20),
	other_reference_sequence TEXT,
	other_reference_text TEXT
);

CREATE TABLE IF NOT EXISTS patview_core.g_patent (
	patent_id VARCHAR(20),
	patent_type VARCHAR(100),
	patent_date VARCHAR(10),
	patent_title TEXT,
	patent_abstract TEXT,
	wipo_kind VARCHAR(10),
	num_claims INTEGER,
	withdrawn INTEGER,
	filename VARCHAR(120)
);

CREATE TABLE IF NOT EXISTS patview_core.g_pct_data (
	patent_id VARCHAR(20),
	published_filed_date VARCHAR(10),
	pct_371_date VARCHAR(10),
	pct_102_date VARCHAR(10),
	filed_country VARCHAR(20),
	application_kind VARCHAR(20),
	pct_doc_number VARCHAR(20),
	pct_doc_type VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS patview_core.g_persistent_assignee (
	rawassignee_uuid VARCHAR(64),
	disamb_assignee_id_20181127  VARCHAR(256),
	disamb_assignee_id_20190312  VARCHAR(256),
	disamb_assignee_id_20190820  VARCHAR(256),
	disamb_assignee_id_20191008  VARCHAR(256),
	disamb_assignee_id_20191231  VARCHAR(256),
	disamb_assignee_id_20200331  VARCHAR(256),
	disamb_assignee_id_20200630  VARCHAR(256),
	disamb_assignee_id_20200929  VARCHAR(256),
	disamb_assignee_id_20201229  VARCHAR(256),
	disamb_assignee_id_20210330  VARCHAR(256),
	disamb_assignee_id_20210629  VARCHAR(256),
	disamb_assignee_id_20210708  VARCHAR(256),
	disamb_assignee_id_20210930  VARCHAR(256),
	disamb_assignee_id_20211230  VARCHAR(256),
	disamb_assignee_id_20220630  VARCHAR(256)
);

CREATE TABLE IF NOT EXISTS patview_core.g_persistent_inventor (
	rawinventor_uuid VARCHAR(64),
    disamb_inventor_id_20170808  VARCHAR(256),
    disamb_inventor_id_20171003  VARCHAR(256),
    disamb_inventor_id_20171226  VARCHAR(256),
    disamb_inventor_id_20180528  VARCHAR(256),
    disamb_inventor_id_20181127  VARCHAR(256),
    disamb_inventor_id_20190312  VARCHAR(256),
    disamb_inventor_id_20190820  VARCHAR(256),
    disamb_inventor_id_20191008  VARCHAR(256),
    disamb_inventor_id_20191231  VARCHAR(256),
    disamb_inventor_id_20200331  VARCHAR(256),
    disamb_inventor_id_20200630  VARCHAR(256),
    disamb_inventor_id_20200929  VARCHAR(256),
    disamb_inventor_id_20201229  VARCHAR(256),
    disamb_inventor_id_20211230  VARCHAR(256),
    disamb_inventor_id_20220630  VARCHAR(256)
);

CREATE TABLE IF NOT EXISTS patview_core.g_rel_app_text (
	patent_id VARCHAR(20),
	rel_app_text TEXT
);

CREATE TABLE IF NOT EXISTS patview_core.g_uspc_at_issue (
	patent_id VARCHAR(20),
	uspc_sequence INTEGER,
	uspc_mainclass_id VARCHAR(20),
	uspc_mainclass_title VARCHAR(256),
	uspc_subclass_id VARCHAR(20),
	uspc_subclass_title VARCHAR(512)
);

CREATE TABLE IF NOT EXISTS patview_core.g_us_application_citation (
	patent_id VARCHAR(20),
	citation_sequence INTEGER,
	citation_document_number VARCHAR(64),
	citation_date VARCHAR(10),
	record_name VARCHAR(128),
	wipo_kind VARCHAR(10),
	citation_category VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS patview_core.g_us_patent_citation (
	patent_id VARCHAR(20),
	citation_sequence INTEGER,
	citation_patent_id VARCHAR(20),
	citation_date VARCHAR(10),
	record_name VARCHAR(128),
	wipo_kind VARCHAR(10),
	citation_category VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS patview_core.g_us_rel_doc (
	patent_id VARCHAR(20),
	related_doc_number VARCHAR(64),
	published_country VARCHAR(20),
	related_doc_type VARCHAR(64),
	related_doc_kind VARCHAR(64),
	related_doc_published_date VARCHAR(10),
	related_doc_status VARCHAR(20),
	related_doc_sequence INTEGER,
	wipo_kind VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS patview_core.g_us_term_of_grant (
	patent_id VARCHAR(20),
	disclaimer_date VARCHAR(10),
	term_disclaimer VARCHAR(128),
	term_grant VARCHAR(128),
	term_extension VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS patview_core.g_wipo_technology (
	patent_id VARCHAR(20),
	wipo_field_sequence INTEGER,
	wipo_field_id DECIMAL,
	wipo_sector_title VARCHAR(60),
	wipo_field_title VARCHAR(255)
);

