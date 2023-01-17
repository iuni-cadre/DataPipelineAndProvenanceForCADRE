-- ############### --
--      NODES      --
-- ############### --

-- Inventor
CREATE TEMP VIEW g_inventor_export AS (
SELECT inventor_id,
        disambig_inventor_name_first,
        disambig_inventor_name_last,
        male_flag,
        -- num_patents no longer supported
        -- num_assignees no longer supported
        location_id
        -- first_seen_date no longer supported
        -- last_seen_date no longer supported
        -- years_active no longer supporter
FROM patview_core.g_inventor_disambiguated);
\copy (SELECT * FROM g_inventor_export) TO 'inventor_nodes.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

-- Location
CREATE TEMP VIEW g_location_export AS (
SELECT location_id,
        disambig_city,
        county,
        disambig_state,
        disambig_country,
        state_fips,
        county_fips,
        latitude::VARCHAR(256),
        longitude::VARCHAR(256)
        -- num_assignees no longer supported
        -- num_invenotrs no longer supported
        -- num_patents no longer supported
FROM patview_core.g_location_disambiguated);
\copy (SELECT * FROM g_location_export) TO 'location_nodes.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

-- Assginee
CREATE TEMP VIEW g_assignee_export AS (
SELECT assignee_id,
        assignee_type,
        disambig_assignee_individual_name_first,
        disambig_assignee_individual_name_last,
        disambig_assignee_organization
        -- num patents no longer supported
        -- num inventors no longer supported
        -- first seen date no longer supported
        -- last seen date no longer supported
        -- years active no longer supported
        -- persistent assignee id no longer supported
FROM patview_core.g_assignee_disambiguated
);
\copy (SELECT * FROM g_assignee_export) TO 'assignee_nodes.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

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
\copy (SELECT * FROM g_gov_org_export where row_num = 1) TO 'gov_org_nodes.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

-- Application
CREATE TEMP VIEW g_application_export AS (
SELECT application_id,
        patent_application_type,
        -- number no longer supported
        -- country no longer supported
        filing_date
FROM patview_core.g_application
);
\copy (SELECT * FROM g_application_export) TO 'application_nodes.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

-- Lawyer 
CREATE TEMP VIEW g_attorney_export AS (
SELECT attorney_id,
        disambig_attorney_name_first,
        disambig_attorney_name_last,
        disambig_attorney_organization
        -- num patents no longer supported
        -- num assignees no longer supported
        -- num inventors no longer supported
        -- first seen date no longer supported
        -- last seen date no longers supported
        -- years active no longer supported
        -- persistent lawyer id no longer supported
FROM patview_core.g_attorney_disambiguated
);
\copy (SELECT * FROM g_attorney_export) TO 'lawyer_nodes.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

-- Patent
CREATE TEMP VIEW g_patent_export AS (
SELECT patent_id,
        patent_type,
        -- number no longer supported
        -- country no longer supported
        patent_date,
        extract(year from date(patent_date)) AS year,
        patent_abstract,
        patent_title,
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
\copy (SELECT * FROM g_patent_export) TO 'patent_nodes.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

-- ############### --
--      EDGES      --
-- ############### --

-- INVENTOR LOCATED IN
\copy (SELECT location_id,inventor_id FROM patview_core.g_inventor_disambiguated) TO 'inventor_located_in.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

-- ASSIGNEE LOCATED IN
\copy (SELECT location_id,assignee_id FROM patview_core.g_assignee_disambiguated) TO 'assignee_located_in.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

-- COINVENTOR OF 
CREATE TEMP VIEW g_coinventor_export AS (
SELECT a.inventor_id as inventor_id,
        b.inventor_id as coinventor_id
FROM patview_core.g_inventor_disambiguated a
JOIN patview_core.g_inventor_disambiguated b
ON a.patent_id = b.patent_id AND a.inventor_id <> b.inventor_id
);
\copy (SELECT * FROM g_coinventor_export) TO 'coinventor_of.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

-- INVENTOR OF


-- Assigned to
\copy (SELECT patent_id,assignee_id FROM patview_core.g_assignee_disambiguated) TO 'assigned_to.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

-- Lawyer of
\copy (SELECT patent_id,attorney_id FROM patview_core.g_attorney_disambiguated) TO 'lawyer_of.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

--Interested in 
CREATE TEMP VIEW g_interested_in_export AS (
SELECT org.patent_id,
        org.gi_organization_id as organization_id
FROM patview_core.g_gov_interest_org org
JOIN patview_core.g_patent p
ON p.patent_id = org.patent_id
);
\copy (SELECT * FROM g_interested_in_export) TO 'interested_in.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

-- Application -> Cites -> Patents
CREATE TEMP VIEW g_app_cites_pat_export AS (
SELECT cit.patent_id,
        app.application_id
FROM patview_core.g_us_application_citation cit
JOIN patview_core.g_application app
ON cit.patent_id = app.patent_id
);
\copy (SELECT * FROM g_app_cites_pat_export) TO 'cites.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

-- Application -> Becomes -> Patent
\copy (SELECT application_id,patent_id FROM patview_core.g_application) TO 'becomes.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

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
\copy (SELECT * FROM g_pat_cite_pat_export) TO 'citation.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

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
\copy (SELECT * FROM g_cpc_export) TO 'cpc_nodes.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

-- CPC Edge
CREATE TEMP VIEW g_cpc_edge_export AS (
SELECT patent_id, cpc_section AS cpc_id
FROM patview_core.g_cpc_current
UNION ALL
SELECT patent_id, cpc_class AS cpc_id
FROM patview_core.g_cpc_current
UNION ALL
SELECT patent_id, cpc_subclass AS cpc_id
FROM patview_core.g_cpc_current
UNION ALL
SELECT patent_id, cpc_group AS cpc_id
FROM patview_core.g_cpc_current
);
\copy (SELECT * FROM g_cpc_edge_export) TO 'cpc_category_of.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

-- USPC Node
CREATE TEMP VIEW g_uspc_export AS (
SELECT DISTINCT(uspc_mainclass_id) AS uspc_id,
        'mainclass' AS level,
        uspc_mainclass_title AS label
FROM patview_core.g_uspc_at_issue
);
\copy (SELECT * FROM g_uspc_export) TO 'uspc_nodes.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

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
\copy (SELECT * FROM g_uspc_edge_export) TO 'uspc_category_of.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

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
\copy (SELECT * FROM g_wipo_export) TO 'wipo_nodes.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

-- WIPO Edge
\copy (SELECT patent_id, wipo_field_id FROM patview_core.g_wipo_technology) TO 'wipo_category_of.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

-- NBER tables removed

-- IPCR Node
CREATE TEMP VIEW g_ipcr_export AS (
SELECT DISTINCT(ipc_sequence) AS ipcr_id, 'section' AS level, section AS label
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence) AS ipcr_id, 'ipc_class' AS level, ipc_class AS label
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence) AS ipcr_id, 'subclass' AS level, subclass AS label
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence) AS ipcr_id, 'main_group' AS level, main_group AS label
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence) AS ipcr_id, 'subgroup' AS level, subgroup AS label
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence) AS ipcr_id, 'symbol_position' AS level, symbol_position AS label
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence) AS ipcr_id, 'classification_value' AS level, classification_value AS label
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence) AS ipcr_id, 'classification_data_source' AS level, classification_data_source AS label
FROM patview_core.g_ipc_at_issue
);
\copy (SELECT * FROM g_ipcr_export) TO 'ipcr_nodes.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;

-- IPCR Edge
\copy (SELECT patent_id, ipc_sequence FROM patview_core.g_ipc_at_issue) TO 'ipcr_category_of.tsv' CSV DELIMITER E'\t' NULL 'NULL' HEADER;