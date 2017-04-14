DROP TABLE IF EXISTS Orphan_Table;
DROP TABLE IF EXISTS Yet_Another_Orphan_Table;
DROP TABLE IF EXISTS Points_To_Leaf_Table;
DROP TABLE IF EXISTS Leaf_Table;
DROP TABLE IF EXISTS Join_Table;
DROP TABLE IF EXISTS Table_A;
DROP TABLE IF EXISTS Table_B;

CREATE TABLE Orphan_Table
(
  id       SERIAL NOT NULL PRIMARY KEY,
  number   INT  NOT NULL
);
GRANT ALL ON Orphan_Table TO <DBUser>;

CREATE TABLE Yet_Another_Orphan_Table
(
  id       SERIAL  NOT NULL PRIMARY KEY,
  number   INT  NOT NULL
);
GRANT ALL ON Yet_Another_Orphan_Table TO <DBUser>;

CREATE TABLE Leaf_Table
(
  id       SERIAL  NOT NULL PRIMARY KEY,
  number   INT  NOT NULL
);
GRANT ALL ON Leaf_Table TO <DBUser>;

CREATE TABLE Points_To_Leaf_Table
(
  id       SERIAL  NOT NULL PRIMARY KEY,
  leafId   INT  NOT NULL,
  FOREIGN KEY (leafId) REFERENCES Leaf_Table (id)
);
GRANT ALL ON Points_To_Leaf_Table TO <DBUser>;

CREATE TABLE Table_A
(
  id       SERIAL  NOT NULL PRIMARY KEY,
  number   INT  NOT NULL
);
GRANT ALL ON Table_A TO <DBUser>;

CREATE TABLE Table_B
(
  id       SERIAL  NOT NULL PRIMARY KEY,
  number   INT  NOT NULL
);
GRANT ALL ON Table_B TO <DBUser>;

CREATE TABLE Join_Table
(
  id         SERIAL  NOT NULL PRIMARY KEY,
  table_a_id INT  NOT NULL,
  table_b_id INT  NOT NULL,
  FOREIGN KEY (table_a_id) REFERENCES Table_A (id),
  FOREIGN KEY (table_b_id) REFERENCES Table_B (id)
);
GRANT ALL ON Join_Table TO <DBUser>;

INSERT INTO Orphan_Table ( number ) VALUES(321);
INSERT INTO Yet_Another_Orphan_Table ( number ) VALUES(321);
INSERT INTO Leaf_Table ( number ) VALUES(321);
INSERT INTO Points_To_Leaf_Table (leafId) SELECT id FROM Leaf_Table WHERE Leaf_Table.number = 321;

INSERT INTO Table_A ( number ) VALUES(321);
INSERT INTO Table_B ( number ) VALUES(321);
INSERT INTO Join_Table (table_a_id, table_b_id) VALUES(1, 1);
