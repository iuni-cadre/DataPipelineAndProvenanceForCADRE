

-- This is just for validation and testing purposes
-- \COPY (SELECT * FROM wos_jg_21.wos_jg WHERE wosid = 'WOS:000442672600001') TO '/home/maahutch/cadre/wos_21_postgres/test.csv' CSV HEADER;


-- This is for deduplication of paper nodes
DELETE FROM wos_jg_21.wos_jg
WHERE wosid IN
(SELECT wosid 
FROM 
(SELECT wosid, 
ROW_NUMBER() OVER( PARTITION BY wosid
ORDER BY wosid) as row_num
FROM wos_jg_21.wos_jg) t
WHERE t.row_num > 1); 

-- This is to add the column of lower-case formatted author names
ALTER TABLE wos_jg_21.wos_jg ADD COLUMN lc_standard_names varchar;
UPDATE wos_jg_21.wos_jg SET lc_standard_names=lower(standardnames); 


-- This is to copy the data to TSV format
\COPY (SELECT wosid, isopenaccess, openaccesstype      , abstract            , fundingtext         , citedreferencecount , full_address        ,reprintaddress      , articlenumber       , publicationyear     , publicationdate     , volume              ,issue               , partnumber          , supplement          , specialissue        , earlyaccessdate     , startpage           , endpage             , numberofpages       , publishercity       , publisheraddress    , publisher           , keywordplus         , conferencedate      , conferencesponsor   , conferencehost      , conferencetitle     , documenttype        , rids                , orcid               , standardnames       , authors             , emailaddress        , papertitle          , journaltitle        , journalabbrev       , journaliso          , issn                , doi                 , eissn               , isbn                , pmid                , conferencelocation  , fundingorgs, lc_standard_names FROM wos_jg_21.wos_jg) TO '/N/project/iuni_cadre/wos/wos_jg_2021/janus_graph/nodes_with_quotes.tsv' CSV DELIMITER E'\t' HEADER;


sed -e 's/"//g' /N/project/iuni_cadre/wos/wos_jg_2021/janus_graph/nodes_with_quotes.tsv > /N/project/iuni_cadre/wos/wos_jg_2021/janus_graph/nodes.tsv

\COPY (SELECT citing, cited FROM wos_jg_21.reference) TO '/N/project/iuni_cadre/wos/wos_jg_2021/janus_graph/refEdges.tsv' CSV DELIMITER E'\t' HEADER;
