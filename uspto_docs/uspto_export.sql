--#############
--### NODES ###
--#############

--Inventor
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
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/nodes/inventor.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';

--Location
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
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/nodes/location.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';

--Assignee
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
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/nodes/assignee.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';

--Government Organization
SELECT * 
FROM government_organization
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/nodes/government_organization.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';

--Examiner
SELECT *
FROM examiner
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/nodes/examiner.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';

--Application
SELECT  application_id,
		type,
		number, 
		country, 
		date, 
FROM application
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/nodes/application.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';

--Lawyer
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
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/nodes/lawyer.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';


--Patent
SELECT  patent_id,
		type, 
		number, 
		country, 
		date, 
		year, 
		abstract, 
		title, 
		kind, 
		num_claims, 
		num_foreign_documents_cited, 
		num_us_applications_cited, 
		num_us_patents_cited, 
		num_total_documents_cited, 
		num_times_cited_by_us_patents, 
		earliest_application_date, 
		patent_processing_days, 
		uspc_current_mainclass_average_patent_processing_days, 
		cpc_current_group_average_patent_processing_days,
		term_extension, 
		detail_desc_length
FROM patent
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/nodes/patent.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';


--#############
--### EDGES ###
--#############

--INVENTOR LOCATED IN
SELECT  location_id,
		inventor_id
FROM  location_inventor
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/edges/inventor_located_in.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';

--ASSIGNEE_LOCATED_IN
SELECT  location_id, 
		assignee_id
FROM location_assignee
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/edges/assignee_located_in.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';


--COINVENTOR_OF
SELECT  inventor_id, 
		coinventor_id
FROM inventor_coinventor
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/edges/coinventor_with.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';


--INVENTOR_OF
SELECT  patent_id, 
		inventor_id
FROM patent_inventor
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/edges/inventor_of.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';

--ASSIGNED_TO
SELECT  patent_id, 
		assignee_id
FROM patent_assignee
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/edges/assigned_to.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';

--LAWYER_OF
SELECT  patent_id,
 		laywer_id
FROM patent_lawyer
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/edges/lawyer_of.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';

--INTERESTED_IN
SELECT  patent_id, 
		organization_id
FROM patent_govintorg
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/edges/interest_in.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';

--EXAMINER_OF
SELECT  patent_id,
		examiner_id, 
FROM patent_examiner
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/edges/examiner_of.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';


--APPLICATION -> CITES -> PATENTS
SELECT  citing_patent_id, 
		cited_application_id
FROM usapplicationcitation
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/edges/app_cites_patents.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';

--APPLICATION -> BECOMES -> PATENT
SELECT  application_id,
		patent_id
FROM application
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/edges/becomes.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';


--PATENT -> CITES -> PATENTS
SELECT 	citing_patent_id, 
		cited_patent_id
FROM uspatentcitation
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/edges/patent_cites_patent.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';


--#####################
--### CATEGORY DATA ###
--#####################


--CPC - Node
SELECT section_id AS 'cpc_id', 'section' AS 'level', section_id AS 'label' FROM cpc_current
UNION ALL
SELECT subsection_id AS 'cpc_id', 'subsection' AS 'level', subsection_title AS 'label' FROM cpc_current 
UNION ALL
SELECT group_id AS 'cpc_id',  'group' AS 'level', group_title AS 'label' FROM cpc_current 
UNION ALL
SELECT subgroup_id AS 'cpc_id', 'subgroup' AS 'level', subgroup_title AS 'label' FROM cpc_current
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/categories/cpc_node.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';

--CPC - Edge
SELECT patent_id, section_id AS 'cpc_id' FROM cpc_current
UNION ALL
SELECT patent_id, subsection_id AS 'cpc_id' FROM cpc_current
UNION ALL
SELECT patent_id, group_id AS 'cpc_id' FROM cpc_current
UNION ALL
SELECT patent_id, subgroup_id AS 'cpc_id' FROM cpc_current
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/categories/cpc_to_patent.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';


--USPC - Node
SELECT mainclass_id AS 'uspc_id', 'mainclass' AS 'level', mainclass_title AS 'label' FROM uspc_current
UNION ALL
SELECT subclass_id AS 'uspc_id', 'subclass' AS 'level', subclass_id AS 'label' FROM uspc_current
INTO OUTFILE '/N/project/mag/uspto_june_2020/csv_for_janusgraph/categories/uspc_node.csv'
FIELDS ENCLOSED BY '"' 
TERMINATED BY ';' 
ESCAPED BY '"' 
LINES TERMINATED BY '\r\n';

--USPC - Edge 
SELECT patent_id, mainclass_id AS 'uspc_id' FROM uspc_current


		







