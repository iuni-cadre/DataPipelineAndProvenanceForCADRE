USE PatentsView_20200630;

-- #############
-- ### NODES ###
-- #############

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
	   name_first, 
   	   name_last, 
 	   num_patents,
	   num_assignees, 
	   lastknown_location_id, 
	   first_seen_date,
	   last_seen_date, 
	   years_active
FROM inventor
INTO OUTFILE '/tmp/csv_for_janusgraph/nodes/inventor.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';

-- Location
SELECT  'location_id', 
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
SELECT  location_id, 
		city, 
		state,
		country, 
		state_fips, 
		county_fips, 
		latitude, 
		longitude,
		num_assignees, 
		num_inventors, 
		num_patents
FROM location
INTO OUTFILE '/tmp/csv_for_janusgraph/nodes/location.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';

-- Assignee
SELECT  'assignee_id',
		'type', 
		'name_first', 
		'name_last', 
		'organization',
		'num_patents', 
		'num_inventors', 
		'first_seen_date', 
		'last_seen_date', 
		'years_active', 
		'persistent_assignee_id'
UNION
SELECT  assignee_id,
		type, 
		name_first, 
		name_last, 
		organization,
		num_patents, 
		num_inventors, 
		first_seen_date, 
		last_seen_date, 
		years_active, 
		persistent_assignee_id
FROM assignee
INTO OUTFILE '/tmp/csv_for_janusgraph/nodes/assignee.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';

-- Government Organization
SELECT 'organization_id', 
	   'name',
	   'level_one',
	   'level_two',
	   'level_three'
UNION
SELECT * 
FROM government_organization
INTO OUTFILE '/tmp/csv_for_janusgraph/nodes/government_organization.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';

-- Examiner
SELECT  'examiner_id',
		'name_first',
		'name_last',
		'role',
		'group',
		'persistent_examiner_id'
UNION	
SELECT *
FROM examiner
INTO OUTFILE '/tmp/csv_for_janusgraph/nodes/examiner.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';

-- Application
SELECT  'application_id',
		'type',
		'number', 
		'country', 
		'date'
UNION
SELECT  application_id,
		type,
		number, 
		country, 
		date
FROM application
INTO OUTFILE '/tmp/csv_for_janusgraph/nodes/application.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';

-- Lawyer
SELECT  'lawyer_id', 
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
SELECT  lawyer_id, 
		name_first, 
		name_last, 
		organization, 
		num_patents, 
		num_assignees, 
		num_inventors, 
		first_seen_date, 
		last_seen_date, 
		years_active, 
		persistent_lawyer_id
FROM lawyer
INTO OUTFILE '/tmp/csv_for_janusgraph/nodes/lawyer.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';


-- Patent
SET NAMES utf8;
SELECT  'patent_id',
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
SELECT  ifnull(patent_id,'NULL'),
		ifnull(type, 'NULL'),
		ifnull(number, 'NULL'),
		ifnull(country, 'NULL'),
		ifnull(date, 'NULL'),
		ifnull(year, 'NULL'),
		ifnull(abstract, 'NULL'),
		ifnull(title, 'NULL'),
		ifnull(kind, 'NULL'),
		ifnull(num_claims,'NULL'), 
		ifnull(num_foreign_documents_cited, 'NULL'),
		ifnull(num_us_applications_cited, 'NULL'),
		ifnull(num_us_patents_cited, 'NULL'),
		ifnull(num_total_documents_cited, 'NULL'),
		ifnull(num_times_cited_by_us_patents, 'NULL'),
		ifnull(earliest_application_date, 'NULL'),
		ifnull(patent_processing_days, 'NULL'),
		ifnull(uspc_current_mainclass_average_patent_processing_days, 'NULL'),
		ifnull(cpc_current_group_average_patent_processing_days, 'NULL'),
		ifnull(term_extension, 'NULL'),
		ifnull(detail_desc_length,'NULL')
FROM patent
INTO OUTFILE '/tmp/csv_for_janusgraph/nodes/patent2.tsv'
FIELDS TERMINATED BY '\t' ENCLOSED BY '"' ESCAPED BY '\\' 
LINES TERMINATED BY '\r\n';


-- #############
-- ### EDGES ###
-- #############

-- INVENTOR LOCATED IN
SELECT  'location_id',
		'inventor_id'
UNION
SELECT  location_id,
		inventor_id
FROM  location_inventor
INTO OUTFILE '/tmp/csv_for_janusgraph/edges/inventor_located_in.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';

-- ASSIGNEE_LOCATED_IN
SELECT  'location_id', 
		'assignee_id'
UNION
SELECT  location_id, 
		assignee_id
FROM location_assignee
INTO OUTFILE '/tmp/csv_for_janusgraph/edges/assignee_located_in.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';


-- COINVENTOR_OF
SELECT  'inventor_id', 
		'coinventor_id'
UNION
SELECT  inventor_id, 
		coinventor_id
FROM inventor_coinventor
INTO OUTFILE '/tmp/csv_for_janusgraph/edges/coinventor_with.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';


-- INVENTOR_OF
SELECT  'patent_id', 
		'inventor_id'
UNION
SELECT  patent_id, 
		inventor_id
FROM patent_inventor
INTO OUTFILE '/tmp/csv_for_janusgraph/edges/inventor_of.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';

-- ASSIGNED_TO
SELECT  'patent_id', 
		'assignee_id'
UNION
SELECT  patent_id, 
		assignee_id
FROM patent_assignee
INTO OUTFILE '/tmp/csv_for_janusgraph/edges/assigned_to.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';

-- LAWYER_OF
SELECT  'patent_id',
 		'lawyer_id'
UNION
SELECT  patent_id,
 		lawyer_id
FROM patent_lawyer
INTO OUTFILE '/tmp/csv_for_janusgraph/edges/lawyer_of.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';

-- INTERESTED_IN
SELECT  'patent_id', 
		'organization_id'
UNION
SELECT  patent_id, 
		organization_id
FROM patent_govintorg
INTO OUTFILE '/tmp/csv_for_janusgraph/edges/interest_in.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';

-- EXAMINER_OF
SELECT  'patent_id',
		'examiner_id'
UNION
SELECT  patent_id,
		examiner_id
FROM patent_examiner
INTO OUTFILE '/tmp/csv_for_janusgraph/edges/examiner_of.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';


-- APPLICATION -> CITES -> PATENTS
SELECT  'citing_patent_id', 
		'cited_application_id'
UNION
SELECT  citing_patent_id, 
		cited_application_id
FROM usapplicationcitation
INTO OUTFILE '/tmp/csv_for_janusgraph/edges/app_cites_patents.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';

-- APPLICATION -> BECOMES -> PATENT
SELECT  'application_id',
		'patent_id'
UNION
SELECT  application_id,
		patent_id
FROM application
INTO OUTFILE '/tmp/csv_for_janusgraph/edges/becomes.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';


-- PATENT -> CITES -> PATENTS
SELECT 	'citing_patent_id', 
		'cited_patent_id'
UNION
SELECT 	citing_patent_id, 
		cited_patent_id
FROM uspatentcitation
INTO OUTFILE '/tmp/csv_for_janusgraph/edges/patent_cites_patent.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';


-- #####################
-- ### CATEGORY DATA ###
-- #####################


-- CPC - Node
SELECT 'cpc_id', 'level', 'label'
UNION
SELECT DISTINCT(section_id) AS 'cpc_id', 'section' AS 'level', section_id AS 'label' FROM cpc_current
UNION ALL
SELECT DISTINCT(subsection_id) AS 'cpc_id', 'subsection' AS 'level', subsection_title AS 'label' FROM cpc_current 
UNION ALL
SELECT DISTINCT(group_id) AS 'cpc_id',  'group' AS 'level', group_title AS 'label' FROM cpc_current 
UNION ALL
SELECT subgroup_id AS 'cpc_id', 'subgroup' AS 'level', subgroup_title AS 'label' FROM cpc_current
INTO OUTFILE '/tmp/csv_for_janusgraph/categories/cpc_node.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';

-- CPC - Edge
SELECT 'patent_id', 'cpc_id'
UNION
SELECT patent_id, section_id AS 'cpc_id' FROM cpc_current
UNION ALL
SELECT patent_id, subsection_id AS 'cpc_id' FROM cpc_current
UNION ALL
SELECT patent_id, group_id AS 'cpc_id' FROM cpc_current
UNION ALL
SELECT patent_id, subgroup_id AS 'cpc_id' FROM cpc_current
INTO OUTFILE '/tmp/csv_for_janusgraph/categories/cpc_to_patent.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';


-- USPC - Node
SELECT 'uspc_id', 'level', 'label'
UNION
SELECT DISTINCT(ifnull(mainclass_id,'')) AS 'uspc_id', 'mainclass' AS 'level', ifnull(mainclass_title,'') AS 'label' FROM uspc_current
UNION ALL
SELECT DISTINCT(ifnull(subclass_id,''))  AS 'uspc_id', 'subclass' AS 'level', ifnull(subclass_id,'') AS 'label' FROM uspc_current
INTO OUTFILE '/tmp/csv_for_janusgraph/categories/uspc_node2.tsv'
FIELDS TERMINATED BY '\t' ENCLOSED BY '"' ESCAPED BY '\\' 
LINES TERMINATED BY '\r\n';

-- USPC - Edge 
SELECT 'patent_id', 'uspc_id'
UNION
SELECT ifnull(patent_id, ''), ifnull(mainclass_id, '') AS 'uspc_id' FROM uspc_current
UNION ALL 
SELECT ifnull(patent_id,''), ifnull(subclass_id,'') AS 'uspc_id' FROM uspc_current
INTO OUTFILE '/tmp/csv_for_janusgraph/categories/uspc_to_patent2.tsv'
FIELDS TERMINATED BY '\t' ENCLOSED BY '"' ESCAPED BY '\\' 
LINES TERMINATED BY '\r\n';

-- WIPO - Node
SELECT 'wipo_field_id', 'level', 'label'
UNION
SELECT DISTINCT(wipo_field_id), 'field_title' AS 'level', wipo_field_title AS 'label' FROM wipo_entity
UNION ALL
SELECT DISTINCT(wipo_field_id), 'sector_title' AS 'level', wipo_sector_title AS 'label' FROM wipo_entity
INTO OUTFILE '/tmp/csv_for_janusgraph/categories/wipo_node.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';	

-- WIPO - Edge
SELECT 'patent_id', 'wipo_field_id'
UNION
SELECT patent_id, wipo_field_id FROM wipo_entity
INTO OUTFILE '/tmp/csv_for_janusgraph/categories/wipo_to_patent.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';	


-- NBER - Node
SELECT 'nber_id', 'level', 'label'
UNION
SELECT DISTINCT(category_id) AS 'nber_id', 'category' AS 'level', category_title AS 'label' FROM nber
UNION ALL 
SELECT DISTINCT(subcategory_id) AS 'nber_id', 'sub_category' AS 'level', subcategory_title AS 'label' FROM nber
INTO OUTFILE '/tmp/csv_for_janusgraph/categories/nber_node.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';	

-- NBER - Edge
SELECT 'patent_id', 'nber_id'
UNION
SELECT patent_id, category_id AS 'nber_id' FROM nber
UNION ALL
SELECT patent_id, subcategory_id AS 'nber_id' FROM nber
INTO OUTFILE '/tmp/csv_for_janusgraph/categories/nber_to_patent.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';	


-- IPCR - Node
SELECT 'ipcr_id', 'level', 'label'
UNION
SELECT DISTINCT(sequence) AS 'ipcr_id', 'section'    	  			 AS 'level',  section    	   			AS 'label' FROM ipcr
UNION ALL
SELECT DISTINCT(sequence) AS 'ipcr_id', 'ipc_class'  	  			 AS 'level', ipc_class  	   			AS 'label' FROM ipcr
UNION ALL 
SELECT DISTINCT(sequence) AS 'ipcr_id', 'subclass'   	  			 AS 'level', subclass   	   			AS 'label' FROM ipcr
UNION ALL
SELECT DISTINCT(sequence) AS 'ipcr_id', 'main_group' 	  			 AS 'level', main_group 	   			AS 'label' FROM ipcr
UNION ALL
SELECT DISTINCT(sequence) AS 'ipcr_id', 'subgroup'   	  			 AS 'level', subgroup   	   			AS 'label' FROM ipcr
UNION ALL 
SELECT DISTINCT(sequence) AS 'ipcr_id', 'symbol_position'            AS 'level', symbol_position 			AS 'label' FROM ipcr
UNION ALL 
SELECT DISTINCT(sequence) AS 'ipcr_id', 'classification_value'       AS 'level', classification_value 		AS 'label' FROM ipcr
UNION ALL
SELECT DISTINCT(sequence) AS 'ipcr_id', 'classification_data_source' AS 'level', classification_data_source AS 'label' FROM ipcr
INTO OUTFILE '/tmp/csv_for_janusgraph/categories/ipcr_node.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';	


-- IPCR - Edge
SELECT 'patent_id', 'sequence'
UNION
SELECT patent_id, sequence FROM ipcr
INTO OUTFILE '/tmp/csv_for_janusgraph/categories/ipcr_to_patent.tsv'
FIELDS ENCLOSED BY '' 
TERMINATED BY '\t' 
ESCAPED BY '' 
LINES TERMINATED BY '\r\n';	
















