SET NOCOUNT ON
;

USE [MASTER]
;

if exists (select * from sysdatabases where name='exclusion')
		drop database exclusion
;

CREATE DATABASE [exclusion];

ALTER DATABASE [exclusion] SET RECOVERY SIMPLE;

set quoted_identifier on;

SET DATEFORMAT mdy;

use [exclusion];

/**
EXEC sp_configure 'contained database authentication', 1
reconfigure
ALTER DATABASE [exclusion] SET containment = partial

DROP LOGIN neo4j
DROP USER IF EXISTS neo4j

CREATE LOGIN neo4j WITH PASSWORD = 'pAssword!'
CREATE USER neo4j FOR LOGIN neo4j

DROP SCHEMA IF EXISTS exclusion

CREATE SCHEMA exclusion AUTHORIZATION neo4j
*/

if exists (select * from sysobjects where id = object_id('dbo.Orphan_Table') and sysstat & 0xf = 3)
	drop table "Orphan_Table"
;
if exists (select * from sysobjects where id = object_id('dbo.Yet_Another_Orphan_Table') and sysstat & 0xf = 3)
	drop table "Yet_Another_Orphan_Table"
;
if exists (select * from sysobjects where id = object_id('dbo.Leaf_Table') and sysstat & 0xf = 3)
	drop table "Leaf_Table"
;
if exists (select * from sysobjects where id = object_id('dbo.Points_To_Leaf_Table') and sysstat & 0xf = 3)
	drop table "Points_To_Leaf_Table"
;
if exists (select * from sysobjects where id = object_id('dbo.Table_A') and sysstat & 0xf = 3)
	drop table "Table_A"
;
if exists (select * from sysobjects where id = object_id('dbo.Table_B') and sysstat & 0xf = 3)
	drop table "Table_B"
;
if exists (select * from sysobjects where id = object_id('dbo.Join_Table') and sysstat & 0xf = 3)
	drop table "Join_Table"
;

CREATE TABLE "Orphan_Table" (
    "id" "int" IDENTITY (1, 1) NOT NULL,
    "number" "int" NOT NULL,
    CONSTRAINT "PK_Orphan_Table" PRIMARY KEY CLUSTERED ("id")
)
;

CREATE TABLE "Yet_Another_Orphan_Table" (
    "id" "int" IDENTITY (1, 1) NOT NULL,
    "number" "int" NOT NULL,
    CONSTRAINT "PK_Another_Orphan_Table" PRIMARY KEY CLUSTERED ("id")
)
;

CREATE TABLE "Leaf_Table" (
    "id" "int" IDENTITY (1, 1) NOT NULL,
    "number" "int" NOT NULL,
    CONSTRAINT "PK_Leaf_Table" PRIMARY KEY CLUSTERED ("id")
)
;

CREATE TABLE "Points_To_Leaf_Table" (
    "id" "int" IDENTITY (1, 1) NOT NULL,
    "leafId" "int" NOT NULL,
    CONSTRAINT "PK_Points_To_Leaf_Table" PRIMARY KEY CLUSTERED ("id"),
    CONSTRAINT "FK_Points_To_Leaf_Table_Leaf_Table" FOREIGN KEY ("leafId") REFERENCES "Leaf_Table" ("id")
)
;

CREATE TABLE "Table_A" (
    "id" "int" IDENTITY (1, 1) NOT NULL,
    "number" "int" NOT NULL,
    CONSTRAINT "PK_Table_A" PRIMARY KEY CLUSTERED ("id")
)
;

CREATE TABLE "Table_B" (
    "id" "int" IDENTITY (1, 1) NOT NULL,
    "number" "int" NOT NULL,
    CONSTRAINT "PK_Table_B" PRIMARY KEY CLUSTERED ("id")
)
;

CREATE TABLE "Join_Table" (
    "id" "int" IDENTITY (1, 1) NOT NULL,
    "table_a_id" "int" NOT NULL,
    "table_b_id" "int" NOT NULL,
    CONSTRAINT "PK_Join_Table" PRIMARY KEY CLUSTERED ("id"),
    CONSTRAINT "FK_Join_Table_Table_A" FOREIGN KEY ("table_a_id") REFERENCES "Table_A" ("id"),
    CONSTRAINT "FK_Join_Table_Table_B" FOREIGN KEY ("table_b_id") REFERENCES "Table_B" ("id")
)
;

INSERT "Orphan_Table" ( "number" ) VALUES(321)
INSERT "Yet_Another_Orphan_Table" ( "number" ) VALUES(321)
INSERT "Leaf_Table" ( "number" ) VALUES(321)
INSERT "Points_To_Leaf_Table" ("leafId") SELECT id FROM "Leaf_Table" WHERE "Leaf_Table"."number" = 321
;

INSERT "Table_A" ( "number" ) VALUES(321)
INSERT "Table_B" ( "number" ) VALUES(321)
INSERT "Join_Table" ("table_a_id", "table_b_id") VALUES(1, 1)
;