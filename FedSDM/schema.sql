DROP TABLE IF EXISTS user;
DROP TABLE IF EXISTS federation;
DROP TABLE IF EXISTS federationsources;
DROP TABLE IF EXISTS datasource;
DROP TABLE IF EXISTS params;
DROP TABLE IF EXISTS rdfmt;
DROP TABLE IF EXISTS rdfmtpredsources;
DROP TABLE IF EXISTS rdfmtsuperclasses;
DROP TABLE IF EXISTS predicate;
DROP TABLE IF EXISTS predsuperproperty;

DROP TABLE IF EXISTS feedbackreport;
DROP TABLE IF EXISTS feedbackdata;


CREATE TABLE feedbackreport (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    userID INTEGER,
    federationID TEXT NOT NULL,
    issueDesc TEXT NOT NULL,
    issueQuery TEXT NOT NULL,
    issueStatus Text NOT NULL DEFAULT 'Open',
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (userID) REFERENCES user(id)
);

CREATE TABLE feedbackdata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    reportID INTEGER NOT NULL,
    projVar TEXT,
    projPred TEXT,
    rowData TEXT,
    FOREIGN KEY (reportID) REFERENCES feedbackreport(id)
);


CREATE TABLE user (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL,
    userrole TEXT  NOT NULL DEFAULT 'User'
);

CREATE TABLE federation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    uri TEXT UNIQUE NOT NULL,
    description TEXT,
    is_public BIT NOT NULL DEFAULT 0,
    owner_id INTEGER NOT NULL,
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (owner_id) REFERENCES user (id)
);

CREATE TABLE federationsources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sourceID INTEGER NOT NULL,
    federationID INTEGER NOT NULL,
    description TEXT,
    FOREIGN KEY (federationID) REFERENCES federation (id) ON DELETE CASCADE,
    FOREIGN KEY (sourceID) REFERENCES datasource (id)  ON DELETE CASCADE
);


CREATE TABLE datasource (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT NOT NULL,
    name TEXT NOT NULL,
    dstype TEXT NOT NULL DEFAULT 'SPARQL_Endpoint',
    keywords TEXT,
    description TEXT ,
    homepage TEXT,
    version TEXT,
    organization TEXT,
    triples INTEGER NOT NULL DEFAULT -1,
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ontology_graph TEXT
);

CREATE TABLE params(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    paramName TEXT NOT NULL,
    paramValue TEXT NOT NULL,
    sourceID INTEGER NOT NULL,
    FOREIGN KEY (sourceID) REFERENCES datasource (id) ON DELETE CASCADE
);

CREATE TABLE rdfmt (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT 'TYPED',
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    description TEXT
);


CREATE TABLE rdfmtsuperclasses (
    id INTEGER PRIMARY KEY  AUTOINCREMENT,
    rdfmtID INTEGER NOT NULL,
    superClassID INTEGER NOT NULL,
    FOREIGN KEY (superClassID) REFERENCES rdfmt (id) ON DELETE CASCADE,
    FOREIGN KEY (rdfmtID) REFERENCES rdfmt (id) ON DELETE CASCADE
);


CREATE TABLE predicate (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    predicate TEXT UNIQUE NOT NULL,
    description TEXT,
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE predsuperproperty (
    id INTEGER PRIMARY KEY  AUTOINCREMENT,
    predicateID INTEGER NOT NULL,
    superPropertyID INTEGER NOT NULL,
    FOREIGN KEY (superPropertyID) REFERENCES predicate (id) ON DELETE CASCADE,
    FOREIGN KEY (predicateID) REFERENCES predicate (id) ON DELETE CASCADE
);


CREATE TABLE rdfmtpredicates (
    id INTEGER PRIMARY KEY  AUTOINCREMENT,
    predicateID INTEGER NOT NULL,
    rdfmtID INTEGER NOT NULL,
    cardinality INTEGER NOT NULL DEFAULT -1,
    FOREIGN KEY (predicateID) REFERENCES predicate (id) ON DELETE CASCADE,
    FOREIGN KEY (rdfmtID) REFERENCES rdfmt (id) ON DELETE CASCADE
);


CREATE TABLE rdfmtpredsources (
    id INTEGER PRIMARY KEY  AUTOINCREMENT,
    sourceID INTEGER NOT NULL,
    rdfmtID INTEGER NOT NULL,
    predicateID INTEGER NOT NULL,
    rangeID INTEGER NOT NULL,
    predrange TEXT,
    cardinality INTEGER NOT NULL DEFAULT -1,
    FOREIGN KEY (predicateID) REFERENCES rdfmtpredicates (predicateID) ON DELETE CASCADE,
    FOREIGN KEY (sourceID) REFERENCES datasource (id) ON DELETE CASCADE,
    FOREIGN KEY (rangeID) REFERENCES rdfmt (id) ON DELETE CASCADE,
    FOREIGN KEY (rdfmtID) REFERENCES rdfmtpredicates (rdfmtID) ON DELETE CASCADE
);

CREATE TABLE notifications(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    user_id INTEGER,
    status TEXT NOT NULL DEFAULT 'In Progress',
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES user (id)
);
