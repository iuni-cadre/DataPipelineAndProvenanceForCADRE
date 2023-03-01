-- ############### --
--      NODES      --
-- ############### --

-- Inventor
CREATE TEMP VIEW g_inventor_export AS (
SELECT inventor_id,
        concat_ws(' ', disambig_inventor_name_first, disambig_inventor_name_last) AS inventor_name,
        male_flag,
        ROW_NUMBER() OVER( PARTITION BY inventor_id, disambig_inventor_name_first, disambig_inventor_name_last, male_flag) AS row_num
        -- num_patents no longer supported
        -- num_assignees no longer supported
        -- first_seen_date no longer supported
        -- last_seen_date no longer supported
        -- years_active no longer supporter
FROM patview_core.g_inventor_disambiguated);
\copy (SELECT inventor_id, inventor_name, male_flag FROM g_inventor_export WHERE row_num=1) TO 'inventor_nodes.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- Location
CREATE TEMP VIEW g_location_export AS (
SELECT location_id,
        disambig_city,
        county,
        disambig_state,
        disambig_country,
        state_fips,
        county_fips,
        latitude::VARCHAR(256) as latitude,
        longitude::VARCHAR(256) as longitude,
        ROW_NUMBER() OVER( PARTITION BY location_id, disambig_city, county, disambig_state, disambig_country, state_fips, county_fips, latitude, longitude) AS row_num
        -- num_assignees no longer supported
        -- num_invenotrs no longer supported
        -- num_patents no longer supported
FROM patview_core.g_location_disambiguated);
\copy (SELECT location_id, disambig_city, county, disambig_state, disambig_country, state_fips, county_fips, latitude, longitude FROM g_location_export WHERE row_num=1) TO 'location_nodes.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- Assginee
CREATE TEMP VIEW g_assignee_export AS (
SELECT assignee_id,
        assignee_type,
        concat_ws(' ', disambig_assignee_individual_name_first, disambig_assignee_individual_name_last) AS assignee_name,
        REGEXP_REPLACE(disambig_assignee_organization,'\n','','g') AS disambig_assignee_organization,
        ROW_NUMBER() OVER( PARTITION BY assignee_id, assignee_type, disambig_assignee_individual_name_first, disambig_assignee_individual_name_last, disambig_assignee_organization) AS row_num
        -- num patents no longer supported
        -- num inventors no longer supported
        -- first seen date no longer supported
        -- last seen date no longer supported
        -- years active no longer supported
        -- persistent assignee id no longer supported
FROM patview_core.g_assignee_disambiguated
);
\copy (SELECT assignee_id, assignee_type, assignee_name, disambig_assignee_organization FROM g_assignee_export WHERE row_num=1) TO 'assignee_nodes.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- Government Organization
CREATE TEMP VIEW g_gov_org_export AS (
SELECT gi_organization_id::VARCHAR(64) as organization_id,
        fedagency_name,
        level_one,
        level_two,
        level_three,
        ROW_NUMBER() OVER( PARTITION BY gi_organization_id,fedagency_name, level_one, level_two, level_three) AS row_num
FROM patview_core.g_gov_interest_org org
);
\copy (SELECT organization_id, fedagency_name, level_one, level_two, level_three FROM g_gov_org_export where row_num = 1) TO 'gov_org_nodes.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- Application
CREATE TEMP VIEW g_application_export AS (
SELECT application_id,
        patent_application_type,
        -- number no longer supported
        -- country no longer supported
        filing_date,
        ROW_NUMBER() OVER( PARTITION BY application_id, patent_application_type, filing_date) AS row_num
FROM patview_core.g_application
);
\copy (SELECT application_id, patent_application_type, filing_date FROM g_application_export WHERE row_num=1) TO 'application_nodes.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- Lawyer 
CREATE TEMP VIEW g_attorney_export AS (
SELECT attorney_id,
        concat_ws(' ', disambig_attorney_name_first, disambig_attorney_name_last) AS attorney_name,
        disambig_attorney_organization,
        ROW_NUMBER() OVER( PARTITION BY attorney_id, disambig_attorney_name_first, disambig_attorney_name_last, disambig_attorney_organization) AS row_num
        -- num patents no longer supported
        -- num assignees no longer supported
        -- num inventors no longer supported
        -- first seen date no longer supported
        -- last seen date no longers supported
        -- years active no longer supported
        -- persistent lawyer id no longer supported
FROM patview_core.g_attorney_disambiguated
);
\copy (SELECT attorney_id, attorney_name, disambig_attorney_organization FROM g_attorney_export WHERE row_num=1) TO 'attorney_nodes.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- Patent
CREATE TEMP VIEW g_patent_export AS (
SELECT patent_id,
        patent_type,
        -- number no longer supported
        -- country no longer supported
        patent_date,
        extract(year from date(patent_date)) AS year,
        REGEXP_REPLACE(REGEXP_REPLACE(patent_abstract,'\t',' t','g'),'\n','','g') AS patent_abstract,
        REGEXP_REPLACE(REGEXP_REPLACE(patent_title,'\t',' t','g'),'\n','','g') AS patent_title,
        wipo_kind,
        num_claims
        -- num foreign documents cited no longer supported
        -- num us applications cited no longer supported
        -- num us patents cited no longer supported
        -- num total documents cited no longer supported
        -- num times cited by us patents no longer supported
        -- earliest application date no longer supported
        -- patent processing days no longer supported
        -- uspc current main class average patent processing days no longer supported
        -- cpc current group average patent processing days no longer supported
        -- term extension no longer supported
        -- detail desc length no longer supported
FROM patview_core.g_patent
);
\copy (SELECT * FROM g_patent_export) TO 'patent_nodes.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- ############### --
--      EDGES      --
-- ############### --

-- INVENTOR LOCATED IN
\copy (SELECT location_id,inventor_id FROM patview_core.g_inventor_disambiguated WHERE location_id IS NOT NULL) TO 'inventor_located_in.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- ASSIGNEE LOCATED IN
\copy (SELECT location_id,assignee_id FROM patview_core.g_assignee_disambiguated WHERE location_id IS NOT NULL) TO 'assignee_located_in.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- COINVENTOR OF 
CREATE TEMP VIEW g_coinventor_export AS (
SELECT a.inventor_id as inventor_id,
        b.inventor_id as coinventor_id
FROM patview_core.g_inventor_disambiguated a
JOIN patview_core.g_inventor_disambiguated b
ON a.patent_id = b.patent_id AND a.inventor_id <> b.inventor_id
);
\copy (SELECT * FROM g_coinventor_export) TO 'coinventor_of.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- INVENTOR OF
\copy (SELECT patent_id, inventor_id FROM patview_core.g_inventor_disambiguated) TO 'inventor_of.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- Assigned to
\copy (SELECT patent_id,assignee_id FROM patview_core.g_assignee_disambiguated) TO 'assigned_to.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- Lawyer of
\copy (SELECT patent_id,attorney_id FROM patview_core.g_attorney_disambiguated) TO 'attorney_of.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

--Interested in 
CREATE TEMP VIEW g_interested_in_export AS (
SELECT org.patent_id,
        org.gi_organization_id as organization_id
FROM patview_core.g_gov_interest_org org
JOIN patview_core.g_patent p
ON p.patent_id = org.patent_id
);
\copy (SELECT * FROM g_interested_in_export) TO 'interested_in.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- Application -> Cites -> Patents
CREATE TEMP VIEW g_app_cites_pat_export AS (
SELECT cit.patent_id,
        app.application_id
FROM patview_core.g_us_application_citation cit
JOIN patview_core.g_application app
ON cit.patent_id = app.patent_id
);
\copy (SELECT * FROM g_app_cites_pat_export) TO 'cites.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- Application -> Becomes -> Patent
\copy (SELECT application_id,patent_id FROM patview_core.g_application) TO 'becomes.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- Patent -> Cites -> Patents
CREATE TEMP VIEW g_pat_cite_pat_export AS (
SELECT c.patent_id,
        c.citation_patent_id
FROM patview_core.g_us_patent_citation c
JOIN patview_core.g_patent p1
ON p1.patent_id = c.patent_id
JOIN patview_core.g_patent p2
ON p2.patent_id = c.citation_patent_id
);
\copy (SELECT * FROM g_pat_cite_pat_export) TO 'citation.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- ############### --
--  CATEGORY DATA  --
-- ############### --

-- CPC Node
CREATE TEMP VIEW g_cpc_export AS (
SELECT DISTINCT(cpc_section) AS cpc_id,
        'section' AS level,
        cpc_section AS label
FROM patview_core.g_cpc_current
UNION ALL
SELECT DISTINCT(c.cpc_class) AS cpc_id,
        'subsection' AS level,
        t.cpc_class_title AS label
FROM patview_core.g_cpc_current c
JOIN patview_core.g_cpc_title t
ON c.cpc_group = t.cpc_group
UNION ALL
SELECT DISTINCT(c.cpc_subclass) AS cpc_id,
        'group' as level,
        t.cpc_subclass_title AS label
FROM patview_core.g_cpc_current c
JOIN patview_core.g_cpc_title t
ON c.cpc_group = t.cpc_group
UNION ALL
SELECT c.cpc_group AS cpc_id,
        'subgroup' AS level,
        t.cpc_group_title AS label
FROM patview_core.g_cpc_current c
JOIN patview_core.g_cpc_title t
ON c.cpc_group = t.cpc_group
);
\copy (SELECT * FROM g_cpc_export) TO 'cpc_nodes.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- CPC Edge
CREATE TEMP VIEW g_cpc_edge_export AS (
SELECT p.patent_id, c.cpc_section AS cpc_id
FROM patview_core.g_cpc_current c
JOIN patview_core.g_cpc_title t
ON c.cpc_group = t.cpc_group
JOIN patview_core.g_patent p
ON p.patent_id = c.patent_id
UNION ALL
SELECT p.patent_id, c.cpc_class AS cpc_id
FROM patview_core.g_cpc_current c
JOIN patview_core.g_cpc_title t
ON c.cpc_group = t.cpc_group
JOIN patview_core.g_patent p
ON p.patent_id = c.patent_id
UNION ALL
SELECT p.patent_id, c.cpc_subclass AS cpc_id
FROM patview_core.g_cpc_current c
JOIN patview_core.g_cpc_title t
ON c.cpc_group = t.cpc_group
JOIN patview_core.g_patent p
ON p.patent_id = c.patent_id
UNION ALL
SELECT p.patent_id, c.cpc_group AS cpc_id
FROM patview_core.g_cpc_current c
JOIN patview_core.g_cpc_title t
ON c.cpc_group = t.cpc_group
JOIN patview_core.g_patent p
ON p.patent_id = c.patent_id
);
\copy (SELECT * FROM g_cpc_edge_export) TO 'cpc_category_of.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- USPC Node
CREATE TEMP VIEW g_uspc_export AS (
SELECT DISTINCT(uspc_mainclass_id) AS uspc_id,
        'mainclass' AS level,
        uspc_mainclass_title AS label
FROM patview_core.g_uspc_at_issue
UNION ALL
SELECT DISTINCT(uspc_subclass_id) AS uspc_id,
        'subclass' AS level,
        REGEXP_REPLACE(uspc_subclass_title, '\t','','g') as label
FROM patview_core.g_uspc_at_issue
);
\copy (SELECT * FROM g_uspc_export) TO 'uspc_nodes.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- USPC Edge
CREATE TEMP VIEW g_uspc_edge_export AS (
SELECT patent_id,
        uspc_mainclass_id AS uspc_id
FROM patview_core.g_uspc_at_issue
UNION ALL
SELECT patent_id,
        uspc_subclass_id AS uspc_id
FROM patview_core.g_uspc_at_issue
);
\copy (SELECT * FROM g_uspc_edge_export) TO 'uspc_category_of.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- WIPO Node
CREATE TEMP VIEW g_wipo_export AS (
SELECT DISTINCT(wipo_field_id), 
        'field_title' AS level,
        wipo_field_title AS label
FROM patview_core.g_wipo_technology
UNION ALL
SELECT DISTINCT(wipo_field_id),
        'sector_title' AS level,
        wipo_sector_title AS label
FROM patview_core.g_wipo_technology
);
\copy (SELECT * FROM g_wipo_export) TO 'wipo_nodes.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- WIPO Edge
\copy (SELECT patent_id, wipo_field_id FROM patview_core.g_wipo_technology) TO 'wipo_category_of.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- NBER tables removed

-- IPCR Node
CREATE TEMP VIEW g_ipcr_export AS (
SELECT DISTINCT(ipc_sequence) AS ipc_id, 'section' AS level, section AS label
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence) AS ipc_id, 'ipc_class' AS level, ipc_class AS label
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence) AS ipc_id, 'subclass' AS level, subclass AS label
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence) AS ipc_id, 'main_group' AS level, main_group AS label
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence) AS ipc_id, 'subgroup' AS level, subgroup AS label
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence) AS ipc_id, 'symbol_position' AS level, symbol_position AS label
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence) AS ipc_id, 'classification_value' AS level, classification_value AS label
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence) AS ipc_id, 'classification_data_source' AS level, classification_data_source AS label
FROM patview_core.g_ipc_at_issue
);
\copy (SELECT * FROM g_ipcr_export) TO 'ipcr_nodes.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;

-- IPCR Edge
\copy (SELECT patent_id, ipc_sequence AS ipc_id FROM patview_core.g_ipc_at_issue) TO 'ipcr_category_of.tsv' CSV DELIMITER E'\t' NULL E'' HEADER;