-- ############### --
--      NODES      --
-- ############### --

-- Inventor
SELECT 'inventor_id',
        'name_first',
        'name_last',
        'male_flag'
        -- 'num_patents',
        -- 'num_assignees',
        'lastknown_location_id'
        -- 'first_seen_date',
        -- 'last_seen_date',
        -- 'years_active'
UNION ALL
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
FROM patview_core.g_inventor_disambiguated;

-- Location
SELECT 'location_id',
        'city',
        'county',
        'state',
        'country',
        'state_fips',
        'county_fips',
        'latitude',
        'longitude'
        --'num_assignees',
        --'num_inventors',
        --'num_patents'
UNION ALL
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
FROM patview_core.g_location_disambiguated;

-- Assginee
SELECT 'assignee_id',
        'type',
        'name_first',
        'name_last',
        'organization'
        --'num_patents',
        --'num_inventor',
        --'first_seen_date',
        --'last_seen_date',
        --'years_active',
        --'persistent_assignee_id'
UNION ALL
SELECT a.assignee_id,
        a.assignee_type::VARCHAR(64),
        a.disambig_assignee_individual_name_first,
        a.disambig_assignee_individual_name_last,
        a.disambig_assignee_organization
        -- num patents no longer supported
        -- num inventors no longer supported
        -- first seen date no longer supported
        -- last seen date no longer supported
        -- years active no longer supported
        p.rawassignee_uuid -- get from g_persistent_assignee
FROM patview_core.g_assignee_disambiguated a
LEFT JOIN patview_core.g_persistent_assignee p
ON a.assignee_id in ( p.disamb_assignee_id_20181127,p.disamb_assignee_id_20190312,p.disamb_assignee_id_20190820,
                        p.disamb_assignee_id_20191008,p.disamb_assignee_id_20191231,p.disamb_assignee_id_20200331,
                        p.disamb_assignee_id_20200630,p.disamb_assignee_id_20200929,p.disamb_assignee_id_20201229,
                        p.disamb_assignee_id_20210330,p.disamb_assignee_id_20210629,p.disamb_assignee_id_20210708,
                        p.disamb_assignee_id_20210930,p.disamb_assignee_id_20211230,p.disamb_assignee_id_20220630);

-- Government Organization
SELECT 'organization_id',
        'name',
        'level_one',
        'level_two',
        'level_three',
        'gi_statement'
UNION ALL
SELECT org.gi_organization_id::VARCHAR(64),
        org.fedagency_name,
        org.level_one,
        org.level_two,
        org.level_three,
        orgint.gi_statement
FROM patview_core.g_gov_interest_org org
LEFT JOIN patview_core.g_gov_interest orgint
ON org.patent_id = orgint.patent_id;

-- Examiner
SELECT --'examiner_id',
        'name_first',
        'name_last',
        'role',
        'group'
        --'persistent_examiner_id'
UNION ALL
SELECT  -- examiner id no longer supported
        raw_examiner_name_first,
        raw_examiner_name_last,
        examiner_role,
        art_group
        -- persistent examiner id no longer supported
FROM patview_core.g_examiner_not_disambiguated;

-- Application
SELECT 'application_id',
        'type',
        --'number',
        --'country',
        'date'
UNION ALL
SELECT application_id,
        patent_application_type
        -- number no longer supported
        -- country no longer supported
        filing_date
FROM patview_core.g_application;

-- Lawyer 
SELECT 'lawyer_id',
        'name_first',
        'name_last',
        'organization'
        --'num_patents',
        --'num_assignees',
        --'num_inventors',
        --'first_seen_date',
        --'last_seen_date',
        --'years_active',
        --'persistent_lawyer_id'
UNION ALL
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
FROM patview_core.g_attorney_disambiguated;

-- Patent
SELECT 'patent_id',
        'type',
        --'number',
        --'country',
        'date',
        'year',
        'abstract',
        'title',
        'kind',
        'num_claims'
        --'num_foreign_documents_cited',
        --'num_us_applications_cited',
        --'num_us_patents_cited',
        --'num_total_documents_cited',
        --'num_times_cited_by_us_patents',
        --'earliest_application_date',
        --'patent_processing_days',
        --'uspc_current_mainclass_average_patent_processing_days',
        --'cpc_current_group_average_patent_processing_days',
        --'term_extension',
        --'detail_desc_length'
UNION ALL
SELECT COALESCE(patent_id,'NULL'),
        COALESCE(patent_type,'NULL'),
        -- number no longer supported
        -- country no longer supported
        COALESCE(patent_date,'NULL'),
        COALESCE(extract(year from date(patent_date))::VARCHAR(4), 'NULL'),
        COALESCE(patent_abstract, 'NULL'),
        COALESCE(patent_title,'NULL'),
        COALESCE(wipo_kind, 'NULL'),
        COALESCE(num_claims::VARCHAR(64), 'NULL')
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
FROM patview_core.g_patent;

-- ############### --
--      EDGES      --
-- ############### --

-- INVENTOR LOCATED IN
SELECT 'location_id',
        'inventor_id'
UNION ALL
SELECT location_id,
        inventor_id
FROM patview_core.g_inventor_disambiguated;

-- ASSIGNEE LOCATED IN
SELECT 'location_id',
        'assignee_id'
UNION ALL
SELECT location_id,
        assignee_id
FROM patview_core.g_assignee_disambiguated;

-- COINVENTOR OF 
SELECT 'inventor_id',
        'coinventor_id'
UNION ALL
SELECT a.inventor_id as inventor_id,
        b.inventor_id as coinventor_id
FROM patview_core.g_inventor_disambiguated a
JOIN patview_core.g_inventor_disambiguated b
ON a.patent_id = b.patent_id AND a.inventor_id <> b.inventor_id;

-- INVENTOR OF
SELECT 'patent_id',
        'inventor_id'
UNION ALL
SELECT patent_id,
        inventor_id
FROM patview_core.g_inventor_disambiguated;

-- Assigned to
SELECT 'patent_id',
        'assignee_id'
UNION ALL
SELECT patent_id,
        assignee_id
FROM patview_core.g_assignee_disambiguated;

-- Lawyer of
SELECT 'patent_id',
        'lawyer_id'
UNION ALL
SELECT patent_id,
        attorney_id
from patview_core.g_attorney_disambiguated;

--Interested in 
SELECT 'patent_id',
        'gi_organization_id'
UNION ALL
SELECT UPPER(org.patent_id::VARCHAR(20)),
        UPPER(org.gi_organization_id::VARCHAR(64))
FROM patview_core.g_gov_interest_org org
JOIN patview_core.g_patent p
ON p.patent_id = org.patent_id;

-- Examiner of
SELECT 'patent_id',
        'examiner_id'
UNION ALL
SELECT e.patent_id,
        'NULL'
FROM patview_core.g_examiner_not_disambiguated e
JOIN patview_core.g_patent p
ON p.patent_id = e.patent_id;

-- Application -> Cites -> Patents
SELECT 'citing_patent_id',
        'cited_application_id'
UNION ALL
SELECT cit.patent_id,
        app.application_id
FROM patview_core.g_us_application_citation cit
JOIN patview_core.g_application app
ON cit.patent_id = app.patent_id;

-- Application -> Becomes -> Patent
SELECT 'application_id',
        'patent_id'
UNION ALL
SELECT application_id,
        patent_id
FROM patview_core.g_application;

-- Patent -> Cites -> Patentes
SELECT 'citing_patent_id',
        'cited_patent_id'
UNION ALL
SELECT UPPER(c.patent_id),
        UPPER(c.citation_patent_id)
FROM patview_core.g_us_patent_citation c
JOIN patview_core.g_patent p1
ON p1.patent_id = c.patent_id
JOIN patview_core.g_patent p2
ON p2.patent_id = c.citation_patent_id;

-- ############### --
--  CATEGORY DATA  --
-- ############### --

-- CPC Node
SELECT 'cpc_id',
        'level',
        'label'
UNION ALL
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
ON c.cpc_group = t.cpc_group;

-- CPC Edge
SELECT 'patent_id', 'cpc_id'
UNION ALL
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
FROM patview_core.g_cpc_current;

-- USPC Node
SELECT 'uspc_id','level','label'
UNION ALL
SELECT DISTINCT(COALESCE(uspc_mainclass_id,'')) AS uspc_id,
        'mainclass' AS level,
        COALESCE(uspc_mainclass_title,'') AS label
FROM patview_core.g_uspc_at_issue;

-- USPC Edge
SELECT 'patent_id', 'uspc_id'
UNION ALL
SELECT COALESCE(patent_id,''),
        COALESCE(uspc_mainclass_id,'') AS uspc_id
FROM patview_core.g_uspc_at_issue
UNION ALL
SELECT COALESCE(patent_id,''),
        COALESCE(uspc_subclass_id,'') AS uspc_id
FROM patview_core.g_uspc_at_issue;

-- WIPO Node
SELECT 'wipo_field_id', 'level', 'label'
UNION ALL
SELECT DISTINCT(wipo_field_id)::VARCHAR(64), 
        'field_title' AS level,
        wipo_field_title AS label
FROM patview_core.g_wipo_technology
UNION ALL
SELECT DISTINCT(wipo_field_id)::VARCHAR(64),
        'sector_title' AS level,
        wipo_sector_title AS label
FROM patview_core.g_wipo_technology;

-- WIPO Edge
SELECT 'patent_id', 'wipo_field_id'
UNION ALL
SELECT patent_id, wipo_field_id::VARCHAR(64)
FROM patview_core.g_wipo_technology;

-- NBER tables removed

-- IPCR Node
SELECT 'ipcr_id', 'level', 'label'
UNION ALL
SELECT DISTINCT(ipc_sequence)::VARCHAR(64) AS ipcr_id, 'section' AS level, section AS label
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence)::VARCHAR(64) AS ipcr_id, 'ipc_class' AS level, ipc_class AS label
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence)::VARCHAR(64) AS ipcr_id, 'subclass' AS level, subclass AS label
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence)::VARCHAR(64) AS ipcr_id, 'main_group' AS level, main_group AS label
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence)::VARCHAR(64) AS ipcr_id, 'subgroup' AS level, subgroup AS label
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence)::VARCHAR(64) AS ipcr_id, 'symbol_position' AS level, symbol_position AS label
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence)::VARCHAR(64) AS ipcr_id, 'classification_value' AS level, classification_value AS label
FROM patview_core.g_ipc_at_issue
UNION ALL
SELECT DISTINCT(ipc_sequence)::VARCHAR(64) AS ipcr_id, 'classification_data_source' AS level, classification_data_source AS label
FROM patview_core.g_ipc_at_issue;

-- IPCR Edge
SELECT 'patent_id','sequence'
UNION ALL
SELECT patent_id, ipc_sequence::VARCHAR(64)
FROM patview_core.g_ipc_at_issue;