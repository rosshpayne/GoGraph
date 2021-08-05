CREATE TABLE Node (
  PKey BYTES(16) NOT NULL,
  Ty STRING(8) NOT NULL,
) PRIMARY KEY(PKey);

CREATE INDEX NodeTy ON Node(Ty);

CREATE TABLE Edges (
  PKey BYTES(16) NOT NULL,
  SortK STRING(64) NOT NULL,
  N INT64 NOT NULL,
  Nd ARRAY<BYTES(16)> NOT NULL,
  Id ARRAY<INT64> NOT NULL,
  XF ARRAY<INT64> NOT NULL,
) PRIMARY KEY(PKey, SortK),
  INTERLEAVE IN PARENT Node ON DELETE CASCADE;


CREATE TABLE NodeScalarData (
  PKey BYTES(16) NOT NULL,
  SortK STRING(64) NOT NULL,
  Ty STRING(8),
  P STRING(32),
  Bl BOOL,
  F FLOAT64,
  S STRING(MAX),
  B BYTES(MAX),
  DT TIMESTAMP,
) PRIMARY KEY(PKey, SortK),
  INTERLEAVE IN PARENT Node ON DELETE CASCADE;

CREATE NULL_FILTERED INDEX NodePredicateN ON NodeScalarData(P,F);
CREATE NULL_FILTERED INDEX NodePredicateS ON NodeScalarData(P,S);
CREATE NULL_FILTERED INDEX NodePredicateB ON NodeScalarData(P,B);

 
Create table PropagatedNodeScalarData (
 PKey   BYTES(16) NOT NULL,
 SortK  STRING(64) NOT NULL,
 LS	ARRAY<STRING(MAX)>,
 LN	ARRAY<INT64>,
 LF	ARRAY<FLOAT64>,
 LBl	ARRAY<BOOL>,
 LB	ARRAY<BYTES(MAX)>,
 R  ARRAY<BYTES(16)> NOT NULL,
 ) PRIMARY KEY (PKey, SortK),
INTERLEAVE IN PARENT Node ON DELETE CASCADE;

Create table testArray (
    id INT64 NOT NULL,
    intarray ARRAY<INT64>,
    strarray ARRAY<STRING(100)>,
    numarray ARRAY<FLOAT64>,
) PRIMARY KEY (id)
