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
        NULL, -- num_patentes no longer supported
        NULL, -- num_assignees no longer supported
        location_id,
        NULL, -- first_seen_date no longer supported
        NULL, -- last_seen_date no longer supported
        NULL -- years_active no longer supporter
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
        NULL, -- num_assignees no longer supported
        NULL, -- num_invenotrs no longer supported
        NULL -- num_patents no longer supported
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
        NULL, -- num patents no longer supported
        NULL, -- num inventors no longer supported
        NULL, -- first seen date no longer supported
        NULL, -- last seen date no longer supported
        NULL, -- years active no longer supported
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
        NULL -- persistent examiner id no longer supported
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
        NULL, -- number no longer supported
        NULL, -- country no longer supported
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
        NULL, -- num patents no longer supported
        NULL, -- num assignees no longer supported
        NULL, -- num inventors no longer supported
        NULL, -- first seen date no longer supported
        NULL, -- last seen date no longers supported
        NULL, -- years active no longer supported
        NULL -- persistent lawyer id no longer supported
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