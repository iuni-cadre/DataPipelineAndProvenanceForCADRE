-- ############### --
--      NODES      --
-- ############### --

-- Inventor
SELECT 'inventor_id',
        'name_first',
        'name_last',
        'num_patents',
        'num_assignees',
        'lastknown_location_id',
        'first_seen_date',
        'last_seen_date',
        'years_active'
UNION
SELECT inventor_id,
        disambig_inventor_name_first,
        disambig_inventor_name_last,
        'NULL', -- num_patentes no longer supported
        'NULL', -- num_assignees no longer supported
        location_id,
        'NULL', -- first_seen_date no longer supported
        'NULL', -- last_seen_date no longer supported
        'NULL' -- years_active no longer supporter
FROM patview_core.g_inventor_disambiguated;

-- Location
SELECT 'location_id',
        'city',
        'state',
        'country',
        'state_fips',
        'county_fips',
        'latitude',
        'longitude',
        'num_assignees',
        'num_inventors',
        'num_patents'
UNION
SELECT location_id,
        disambig_city,
        disambig_state,
        disambig_country,
        state_fips,
        county_fips,
        latitude,
        longitude,
        'NULL', -- num_assignees no longer supported
        'NULL', -- num_invenotrs no longer supported
        'NULL' -- num_patents no longer supported
FROM patview_core.g_location_disambiguated;

-- Assginee
SELECT 'assignee_id',
        'type',
        'name_first',
        'name_last',
        'organization',
        'num_patents',
        'num_inventor',
        'first_seen_date',
        'last_seen_date',
        'years_active',
        'persistent_assignee_id'
UNION
SELECT a.assignee_id,
        a.assignee_type,
        a.disambig_assignee_individual_name_first,
        a.disambig_assignee_individual_name_last,
        a.disambig_assignee_organization,
        'NULL', -- num patents no longer supported
        'NULL', -- num inventors no longer supported
        'NULL', -- first seen date no longer supported
        'NULL', -- last seen date no longer supported
        'NULL', -- years active no longer supported
        p.rawassignee_uuid, -- get from g_persistent_assignee
FROM patview_core.g_assignee_disambiguated a
LEFT JOIN patview_core.g_persistent_assignee p
ON a.assignee_id = p.disamb_inventor_id_xxxxxxxx;

-- Government Organization
SELECT 'organization_id',
        'name',
        'level_one',
        'level_two',
        'level_three',
        'gi_statement'
UNION
SELECT org.gi_organization_id,
        org.name,
        org.level_one,
        org.level_two,
        org.level_three,
        orgint.gi_statement
FROM patview_core.g_gov_interest_org org
LEFT JOIN patview_core.g_gov_interest orgint
ON org.patent_id = orgint.patent_id;

-- Examiner
SELECT 'examiner_id',
        'name_first',
        'name_last',
        'role',
        'group',
        'persistent_examiner_id'
UNION
SELECT NULL, -- examiner id no longer supported
        raw_examiner_name_first,
        raw_applicant_name_last,
        examiner_role,
        art_group,
        'NULL' -- persistent examiner id no longer supported
FROM patview_core.g_examiner_not_disambiguated;

-- Application
SELECT 'application_id',
        'type',
        'number',
        'country',
        'date'
UNION
SELECT application_id,
        patent_application_type,
        'NULL', -- number no longer supported
        'NULL', -- country no longer supported
        filing_date
FROM patview_core.g_application;

-- Lawyer 
SELECT 'lawyer_id',
        'name_first',
        'name_last',
        'organization',
        'num_patents',
        'num_assignees',
        'num_inventors',
        'first_seen_date',
        'last_seen_date',
        'years_active',
        'persistent_lawyer_id'
UNION
SELECT attorney_id,
        disambig_attorney_name_first,
        disambig_attorney_name_last,
        disambig_attorney_organization,
        'NULL', -- num patents no longer supported
        'NULL', -- num assignees no longer supported
        'NULL', -- num inventors no longer supported
        'NULL', -- first seen date no longer supported
        'NULL', -- last seen date no longers supported
        'NULL', -- years active no longer supported
        'NULL' -- persistent lawyer id no longer supported
FROM patview_core.g_attorney_disambiguated;

-- Patent
SELECT 'patent_id',
        'type',
        'number',
        'country',
        'date',
        'year',
        'abstract',
        'title',
        'kind',
        'num_claims',
        'num_foreign_documents_cited',
        'num_us_applications_cited',
        'num_us_patents_cited',
        'num_total_documents_cited',
        'num_times_cited_by_us_patents',
        'earliest_application_date',
        'patent_processing_days',
        'uspc_current_mainclass_average_patent_processing_days',
        'cpc_current_group_average_patent_processing_days',
        'term_extension',
        'detail_desc_length'
UNION
SELECT ifnull(patent_id,'NULL'),
        ifnull(patent_type,'NULL'),
        'NULL', -- number no longer supported
        'NULL', -- country no longer supported
        ifnull(patent_date,'NULL'),
        ifnull(extract(year from date(patent_date)), 'NULL'),
        ifnull(patent_abstract, 'NULL'),
        ifnull(patent_title,'NULL'),
        ifnull(wipo_kind, 'NULL'),
        ifnull(num_claims, 'NULL'),
        'NULL', -- num foreign documents cited no longer supported
        'NULL', -- num us applications cited no longer supported
        'NULL', -- num us patents cited no longer supported
        'NULL', -- num total documents cited no longer supported
        'NULL', -- num times cited by us patents no longer supported
        'NULL', -- earliest application date no longer supported
        'NULL', -- patent processing days
        'NULL', -- uspc current main class average patent processing days no longer supported
        'NULL', -- cpc current group average patent processing days no longer supported
        'NULL', -- term extension no longer supported
        'NULL' -- detail desc length no longer supported
FROM patview_core.g_patent;

-- ############### --
--      EDGES      --
-- ############### --

-- INVENTOR LOCATED IN
SELECT 'location_id',
        'inventor_id'
UNION
SELECT location_id,
        inventor_id
FROM patview_core.g_inventor_disambiguated;

-- ASSIGNEE LOCATED IN
SELECT 'location_id',
        'assignee_id'
UNION
SELECT location_id,
        assignee_id
FROM patview_core.g_assignee_disambiguated;

-- COINVENTOR OF 
SELECT 'inventor_id',
        'coinventor_id'
UNION
SELECT a.inventor_id as inventor_id,
        b.inventor_id as coinventor_id
FROM patview_core.g_inventor_disambiguated a
JOIN patview_core.g_inventor_disambiguated b
ON a.patent_id = b.patent_id AND a.inventor_id <> b.inventor_id;

-- INVENTOR OF
SELECT 'patent_id',
        'inventor_id'
UNION
SELECT patent_id,
        inventor_id
FROM patview_core.g_inventor_disambiguated;

-- Assigned to
SELECT 'patent_id',
        'assignee_id'
UNION
SELECT patent_id,
        assignee_id
FROM patview_core.g_assignee_disambiguated;

-- Lawyer of
SELECT 'patent_id',
        'lawyer_id'
UNION
SELECT patent_id,
        attorney_id
from patview_core.g_attorney_disambiguated;

--Interested in 
SELECT 'patent_id',
        'gi_organization_id'
UNION
SELECT UPPER(org.patent_id),
        UPPER(org.gi_organization_id)
FROM patview_core.g_gov_interest_org org
JOIN g_patent p
ON p.patent_id = org.patent_id;

-- Examiner of
SELECT 'patent_id',
        'examiner_id'
UNION
SELECT e.patent_id,
        'NULL'
FROM patview_core.g_examiner_not_disambiguated e
JOIN patview_core.g_patent p
ON p.patent_id = e.patent_id;

-- Application -> Cites -> Patents
SELECT 'citing_patent_id',
        'cited_application_id'
UNION
SELECT cit.patent_id,
        app.application_id
FROM patview_core.g_us_application_citation cit
JOIN patview_core.g_application app
ON cit.patent_id = app.patent_id;

-- Application -> Becomes -> Patent
SELECT 'application_id',
        'patent_id'
UNION
SELECT application_id,
        patent_id
FROM patview_core.application;

-- Patent -> Cites -> Patentes
SELECT 'citing_patent_id',
        'cited_patent_id'
UNION
SELECT UPPER(c.patent_id),
        UPPER(c.citation_patent_id)
FROM patview_core.g_us_patent_citation c
JOIN patent p1
ON p1.patent = c.patent_id
JOIN patent p2
ON p2 = c.citation_patent_id;

-- ############### --
--  CATEGORY DATA  --
-- ############### --

-- CPC Node
SELECT 'cpc_id',
        'level',
        'label'
UNION
SELECT DISTINCT(cpc_section) AS 'cpc_id',
        'section' AS 'level',
        cpc_section AS 'label'
FROM patview_core.g_cpc_current
UNION ALL
SELECT DISTINCT(c.cpc_class) AS 'cpc_id',
        'subsection' AS 'level',
        t.cpc_class_title AS 'label'
FROM patview_core.g_cpc_current c
JOIN patview_core.g_cpc_title t
ON c.cpc_group = t.cpc_group
UNION ALL
SELECT DISTINCT(c.cpc_subclass) AS 'cpc_id',
        'group' as 'level',
        t.cpc_subclass_title AS 'label'
FROM patview_core.g_cpc_current c
JOIN patview_core.g_cpc_title t
ON c.cpc_group = t.cpc_group
UNION ALL
SELECT c.cpc_group AS 'cpc_id',
        'subgroup' AS 'level',
        t.cpc_group_title AS 'label'
FROM patview_core.g_cpc_current c
JOIN patview_core.g_cpc_title;

-- CPC Edge
SELECT 'patent_id', 'cpc_id'
UNION
SELECT patent_id, cpc_section as 'cpc_id'
FROM patview_core.g_cpc_current
UNION
SELECT patent_id, cpc_class AS 'cpc_id'
FROM patview_core.g_cpc_current
UNION
SELECT patent_id, cpc_subclass AS 'cpc_id'
FROM patview_core.g_cpc_current
UNION
SELECT patent_id, cpc_group AS 'cpc_id'
FROM patview_core.g_cpc_current;

-- USPC Node
SELECT 'uspc_id','level','label'
UNION
SELECT DISTINCT(ifnull(uspc_mainclass_id,'')) AS 'uspc_id',
        'mainclass' AS 'level',
        ifnull(uspc_mainclass_title,'') AS 'label'
FROM patview_core.g_uspc_at_issue;

-- USPC Edge
SELECT 'patent_id', 'uspc_id'
UNION
SELECT ifnull(patent_id,''),
        ifnull(uspc_mainclass_id,'') AS 'uspc_id'
FROM patview_core.g_uspc_at_issue
UNION ALL
SELECT ifnull(patent_id,''),
        ifnull(uspc_subclass_id,'') AS 'uspc_id'
FROM patview_core.g_uspc_at_issue;

-- WIPO Node
SELECT 'wipo_field_id', 'level', 'label'
UNION
SELECT DISTINCT(wipo_field_id), 
        'field_title' AS 'level',
        wipo_field_title AS 'label'
FROM patview_core.g_wipo_technology
UNION ALL
SELECT DISTINCT(wipo_field_id),
        'sector_title' AS 'level',
        wipo_sector_title AS 'label'
FROM patview_core.g_wipo_technology;

-- WIPO Edge
SELECT 'patent_id', 'wipo_field_id'
UNION
SELECT patent_id, wipo_field_id
FROM patview_core.g_wipo_technology;

-- NBER tables removed

-- IPCR Node
SELECT 'ipcr_id', 'level', 'label'
UNION
SELECT DISTINCT(ipc_sequence) AS 'ipcr_id', 'section' AS 'level', section AS 'label'
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence) AS 'ipcr_id', 'ipc_class' AS 'level', ipc_class AS 'label'
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence) AS 'ipcr_id', 'subclass' AS 'level', subclass AS 'label'
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence) AS 'ipcr_id', 'main_group' AS 'level', main_group AS 'label'
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence) AS 'ipcr_id', 'subgroup' AS 'level', subgroup AS 'label'
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence) AS 'ipcr_id', 'symbol_position' AS 'level', symbol_position AS 'label'
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence) AS 'ipcr_id', 'classification_value' AS 'level', classification_value AS 'label'
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence) AS 'ipcr_id', 'classification_data_source' AS 'level', classification_data_source AS 'label'
FROM patview_core.g_ipc_at_issue;

-- IPCR Edge
SELECT 'patent_id','sequence'
UNION
SELECT patent_id, ipc_sequence
FROM patview_core.g_ipc_at_issue;