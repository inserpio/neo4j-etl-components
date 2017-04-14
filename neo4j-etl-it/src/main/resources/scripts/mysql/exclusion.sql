DROP DATABASE IF EXISTS exclusion;
CREATE DATABASE exclusion
  DEFAULT CHARACTER SET utf8
  COLLATE utf8_unicode_ci;
GRANT ALL ON exclusion.* TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';

CREATE TABLE exclusion.Orphan_Table
(
  id       INT  NOT NULL AUTO_INCREMENT PRIMARY KEY,
  number   INT  NOT NULL
);
GRANT ALL ON exclusion.Orphan_Table TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';

CREATE TABLE exclusion.Yet_Another_Orphan_Table
(
  id       INT  NOT NULL AUTO_INCREMENT PRIMARY KEY,
  number   INT  NOT NULL
);
GRANT ALL ON exclusion.Yet_Another_Orphan_Table TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';

CREATE TABLE exclusion.Leaf_Table
(
  id       INT  NOT NULL AUTO_INCREMENT PRIMARY KEY,
  number   INT  NOT NULL
);
GRANT ALL ON exclusion.Leaf_Table TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';

CREATE TABLE exclusion.Points_To_Leaf_Table
(
  id       INT  NOT NULL AUTO_INCREMENT PRIMARY KEY,
  leafId   INT  NOT NULL,
  FOREIGN KEY (leafId) REFERENCES exclusion.Leaf_Table (id)
);
GRANT ALL ON exclusion.Points_To_Leaf_Table TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';

CREATE TABLE exclusion.Table_A
(
  id       INT  NOT NULL AUTO_INCREMENT PRIMARY KEY,
  number   INT  NOT NULL
);
GRANT ALL ON exclusion.Table_A TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';

CREATE TABLE exclusion.Table_B
(
  id       INT  NOT NULL AUTO_INCREMENT PRIMARY KEY,
  number   INT  NOT NULL
);
GRANT ALL ON exclusion.Table_B TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';

CREATE TABLE exclusion.Join_Table
(
  id         INT  NOT NULL AUTO_INCREMENT PRIMARY KEY,
  table_a_id INT  NOT NULL,
  table_b_id INT  NOT NULL,
  FOREIGN KEY (table_a_id) REFERENCES exclusion.Table_A (id),
  FOREIGN KEY (table_b_id) REFERENCES exclusion.Table_B (id)
);
GRANT ALL ON exclusion.Join_Table TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';

INSERT INTO exclusion.Orphan_Table ( number ) VALUES(321);
INSERT INTO exclusion.Yet_Another_Orphan_Table ( number ) VALUES(321);
INSERT INTO exclusion.Leaf_Table ( number ) VALUES(321);
INSERT INTO exclusion.Points_To_Leaf_Table (leafId) SELECT id FROM exclusion.Leaf_Table WHERE exclusion.Leaf_Table.number = 321;

INSERT INTO exclusion.Table_A ( number ) VALUES(321);
INSERT INTO exclusion.Table_B ( number ) VALUES(321);
INSERT INTO exclusion.Join_Table (table_a_id, table_b_id) VALUES(1, 1);
