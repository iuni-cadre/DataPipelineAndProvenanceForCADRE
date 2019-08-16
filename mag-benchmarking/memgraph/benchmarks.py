from neo4j import GraphDatabase
from datetime import datetime, timedelta

QUERIES = [
    ('node count', 'MATCH (n) RETURN COUNT(n)'),
    ('PAPER node count', 'MATCH (n:PAPER) RETURN COUNT(n)'),
    ('AUTHOR node count', 'MATCH (n:AUTHOR) RETURN COUNT(n)'),
    ('AFFILIATION node count', 'MATCH (n:AFFILIATION) RETURN COUNT(n)'),
    ('JOURNAL node count', 'MATCH (n:JOURNAL) RETURN COUNT(n)'),
    ('FIELDOFSTUDY node count', 'MATCH (n:FIELDOFSTUDY) RETURN COUNT(n)'),
    ('CONFERENCEINSTANCE node count', 'MATCH (n:CONFERENCEINSTANCE) RETURN COUNT(n)'),
    ('CONFERENCESERIES node count', 'MATCH (n:CONFERENCESERIES) RETURN COUNT(n)'),
    ('edge count', 'MATCH ()-[e]-() RETURN COUNT(e)'),
    ('IS_AFFILIATED_WITH edge count', 'MATCH ()-[e:IS_AFFILIATED_WITH]-() RETURN COUNT(e)'),
    ('IS_AUTHOR_OF edge count', 'MATCH ()-[e:IS_AUTHOR_OF]-() RETURN COUNT(e)'),
    ('IS_INSTANCE_OF edge count', 'MATCH ()-[e:IS_INSTANCE_OF]-() RETURN COUNT(e)'),
    ('BELONGS_TO edge count', 'MATCH ()-[e:BELONGS_TO]-() RETURN COUNT(e)'),
    ('IS_PRESENTED_AT edge count', 'MATCH ()-[e:IS_PRESENTED_AT]-() RETURN COUNT(e)'),
    ('IS_PUBLISHED_IN edge count', 'MATCH ()-[e:IS_PUBLISHED_IN]-() RETURN COUNT(e)'),
    ('REFERENCES edge count', 'MATCH ()-[e:REFERENCES]-() RETURN COUNT(e)'),
    ('find specific paper', '''
    MATCH (p:PAPER)
    WHERE p.PaperTitle = 'big data technologies a survey'
    RETURN p'''),
    ('find all authors of a specific paper', '''
    MATCH (p:PAPER)-[:IS_AUTHOR_OF]-(a:AUTHOR)
    WHERE p.PaperTitle = 'big data technologies a survey'
    RETURN a.NormalizedName'''),
    ('find the fields of study a journal\'s publications belong to', '''
    MATCH (j:JOURNAL)-[:IS_PUBLISHED_IN]-(p:PAPER)-[:BELONGS_TO]-(f:FIELDOFSTUDY)
    WHERE j.NormalizedName = 'journal of king saud university computer and information sciences'
    RETURN f.DisplayName'''),
    ('find all authors with papers matching the title criteria', '''
    MATCH (p:PAPER)-[:IS_AUTHOR_OF]-(a:AUTHOR)
    WHERE p.PaperTitle CONTAINS 'big data'
    RETURN a.NormalizedName'''),
    ('find all citations for a given paper', '''
    MATCH (referrer:PAPER)-[e:REFERENCES]->(target:PAPER)
    WHERE target.PaperTitle = 'big data technologies a survey'
    RETURN referrer.PaperTitle'''),
    ('find all citations that are once remove from a given paper P, i.e. the papers citing the papers citing P', '''
    MATCH (referrer:PAPER)-[e:REFERENCES*2]->(target:PAPER) 
    WHERE target.PaperTitle = 'big data technologies a survey' 
    RETURN referrer.PaperTitle'''),
    ('find all citations that are 1-4 times removed from a given paper', '''
    MATCH (referrer:PAPER)-[e:REFERENCES*1..4]->(target:PAPER) 
    WHERE target.PaperTitle = 'big data technologies a survey' 
    RETURN referrer.PaperTitle'''),
    ('find all references for a given paper', '''
    MATCH (referrer:PAPER)-[e:REFERENCES]->(target:PAPER)
    WHERE referrer.PaperTitle = 'big data technologies a survey'
    RETURN target.PaperTitle'''),
    ('find the citations for all papers in a given field of study', '''
    MATCH (f:FIELDOFSTUDY)<-[a:BELONGS_TO]-(target:PAPER)<-[b:REFERENCES]-(referrer:PAPER)
    WHERE f.NormalizedName = 'data processing'
    RETURN COUNT(referrer.PaperTitle)'''),
    ('find all citations once removed from a given paper', '''
    MATCH (target:PAPER)<-[r1:REFERENCES]-(referrer:PAPER)
    WHERE target.PaperTitle = 'big data technologies a survey'
    WITH referrer
    MATCH (referrer)<-[r2:REFERENCES]-(ancestor)
    RETURN COUNT(ancestor)
    ''')
]


def time_query(tx, query):
    start = datetime.now()
    r = tx.run(query).value()
    end = datetime.now()
    return end - start, r


if __name__ == '__main__':
    driver = GraphDatabase.driver('bolt:localhost:7687')

    with driver.session() as session:
        for desc, q in QUERIES:
            print('Query: {}: {}'.format(desc, q))
            elapsed, result = session.read_transaction(time_query, q)
            print('Elapsed: {}'.format(elapsed))
