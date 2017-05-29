CREATE TABLE "Orphan_Table"
(
  "id"     INT  NOT NULL PRIMARY KEY,
  "number" INT  NOT NULL
);

CREATE TABLE "Yet_Another_Orphan_Table"
(
  "id"     INT  NOT NULL PRIMARY KEY,
  "number" INT  NOT NULL
);

CREATE TABLE "Leaf_Table"
(
  "id"     INT  NOT NULL PRIMARY KEY,
  "number" INT  NOT NULL
);

CREATE TABLE "Points_To_Leaf_Table"
(
  "id"     INT  NOT NULL PRIMARY KEY,
  "leafId" INT  NOT NULL,
  FOREIGN KEY ("leafId") REFERENCES "Leaf_Table" ("id")
);

CREATE TABLE "Table_A"
(
  "id"     INT  NOT NULL PRIMARY KEY,
  "number" INT  NOT NULL
);

CREATE TABLE "Table_B"
(
  "id"     INT  NOT NULL PRIMARY KEY,
  "number" INT  NOT NULL
);

CREATE TABLE "Join_Table"
(
  "id"         INT  NOT NULL PRIMARY KEY,
  "table_a_id" INT  NOT NULL,
  "table_b_id" INT  NOT NULL,
  FOREIGN KEY ("table_a_id") REFERENCES "Table_A" ("id"),
  FOREIGN KEY ("table_b_id") REFERENCES "Table_B" ("id")
);

INSERT INTO "Orphan_Table" ( "id", "number" ) VALUES(1, 321);
INSERT INTO "Yet_Another_Orphan_Table" ( "id", "number" ) VALUES(1, 321);
INSERT INTO "Leaf_Table" ( "id", "number" ) VALUES(1, 321);
INSERT INTO "Points_To_Leaf_Table" ( "id", "leafId" ) SELECT 1, "id" FROM "Leaf_Table" WHERE "Leaf_Table"."number" = 321;

INSERT INTO "Table_A" ( "id", "number" ) VALUES(1, 321);
INSERT INTO "Table_B" ( "id", "number" ) VALUES(1, 321);
INSERT INTO "Join_Table" ( "id", "table_a_id", "table_b_id" ) VALUES(1, 1, 1);
