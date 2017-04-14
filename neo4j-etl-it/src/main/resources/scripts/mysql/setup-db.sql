DROP DATABASE IF EXISTS javabase;
CREATE DATABASE javabase
  DEFAULT CHARACTER SET utf8
  COLLATE utf8_unicode_ci;
GRANT ALL ON javabase.* TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';

CREATE TABLE javabase.Address
(
  id       INT  NOT NULL AUTO_INCREMENT PRIMARY KEY,
  postcode TEXT NOT NULL
);
GRANT ALL ON javabase.Address TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';

CREATE TABLE javabase.Person
(
  id        INT  NOT NULL AUTO_INCREMENT PRIMARY KEY,
  username  TEXT NOT NULL,
  addressId INT  NOT NULL,
  FOREIGN KEY (addressId) REFERENCES javabase.Address (id)
);
GRANT ALL ON javabase.Person TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';

CREATE TABLE javabase.Numeric_Table
(
  id              INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  tinyint_field   TINYINT(4),
  smallint_field  SMALLINT,
  mediumint_field MEDIUMINT,
  bigint_field    BIGINT,
  float_field     FLOAT,
  double_field    DOUBLE,
  decimal_field   DECIMAL(8,2)
);
GRANT ALL ON javabase.Numeric_Table TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';

CREATE TABLE javabase.String_Table
(
  id               INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  numericId        INT NOT NULL,
  FOREIGN KEY (numericId) REFERENCES javabase.Numeric_Table (id),
  char_field       CHAR(20),
  text_field       TEXT,
  blob_field       BLOB,
  tinytext_field   TINYTEXT,
  tinyblob_field   TINYBLOB,
  mediumtext_field MEDIUMTEXT,
  mediumblob_field MEDIUMBLOB,
  longtext_field   LONGTEXT,
  longblob_field   LONGBLOB,
  enum_field       ENUM('val-1', 'val-2'),
  varchar_field    VARCHAR(200)
);
GRANT ALL ON javabase.String_Table TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';


CREATE TABLE javabase.Date_Table
(
  id              INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  numericId       INT NOT NULL,
  FOREIGN KEY (numericId) REFERENCES javabase.Numeric_Table (id),
  date_field      DATE,
  datetime_field  DATETIME,
  timestamp_field TIMESTAMP,
  time_field      TIME,
  year_field      YEAR(4)
);
GRANT ALL ON javabase.Date_Table TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';

CREATE TABLE javabase.Publisher
(
  id   INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
  name VARCHAR(20)                    NOT NULL
);
GRANT ALL ON javabase.Publisher TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';

CREATE TABLE javabase.Author
(
  first_name VARCHAR(20) NOT NULL,
  last_name  VARCHAR(20) NOT NULL,
  age        INT,
  PRIMARY KEY (first_name, last_name)
);
GRANT ALL ON javabase.Author TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';

CREATE TABLE javabase.Author_Publisher
(
  author_first_name VARCHAR(20) NOT NULL,
  author_last_name  VARCHAR(20) NOT NULL,
  publisherId       INT         NOT NULL,
  start_year        INT,
  end_year          INT,
  FOREIGN KEY (author_first_name, author_last_name) REFERENCES javabase.Author (first_name, last_name),
  FOREIGN KEY (publisherId) REFERENCES javabase.Publisher (id)
);
GRANT ALL ON javabase.Author_Publisher TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';

CREATE TABLE javabase.Book
(
  id                INT  NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name              TEXT NOT NULL,
  author_first_name VARCHAR(20),
  author_last_name  VARCHAR(20),
  reference_book_id INT,
  FOREIGN KEY (author_first_name, author_last_name) REFERENCES javabase.Author (first_name, last_name),
  FOREIGN KEY (reference_book_id) REFERENCES javabase.Book (id)
);
GRANT ALL ON javabase.Book TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';

CREATE TABLE javabase.Student
(
  id       INT  NOT NULL AUTO_INCREMENT PRIMARY KEY,
  username TEXT NOT NULL
);
GRANT ALL ON javabase.Student TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';

CREATE TABLE javabase.Course
(
  id   INT  NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name TEXT NOT NULL
);
GRANT ALL ON javabase.Course TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';

CREATE TABLE javabase.Student_Course
(
  studentId INT NOT NULL,
  courseId  INT NOT NULL,
  credits   INT NOT NULL,
  FOREIGN KEY (studentId) REFERENCES javabase.Student (id),
  FOREIGN KEY (courseId) REFERENCES javabase.Course (id)
);
GRANT ALL ON javabase.Student_Course TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';

CREATE TABLE javabase.Team
(
  name VARCHAR(20) NOT NULL,
  teamMember1Id       INT         NOT NULL,
  teamMember2Id       INT         NOT NULL,
  teamMember3Id       INT         NOT NULL,
  FOREIGN KEY (teamMember1Id) REFERENCES javabase.Student (id),
  FOREIGN KEY (teamMember2Id) REFERENCES javabase.Student (id),
  FOREIGN KEY (teamMember3Id) REFERENCES javabase.Student (id)
);
GRANT ALL ON javabase.Team TO '<DBUser>'@'localhost'
IDENTIFIED BY '<DBPassword>';

INSERT INTO javabase.Address (postcode) VALUES ('AB12 1XY');
INSERT INTO javabase.Address (postcode) VALUES ('XY98 9BA');
INSERT INTO javabase.Address (postcode) VALUES ('ZZ1 0MN');
INSERT INTO javabase.Address (postcode) VALUES ('ZZ1
4M"N');

INSERT INTO javabase.Person (username, addressId) SELECT
                                                    'user-1',
                                                    id
                                                  FROM javabase.Address
                                                  WHERE javabase.Address.postcode = 'AB12 1XY';
INSERT INTO javabase.Person (username, addressId) SELECT
                                                    'user-2',
                                                    id
                                                  FROM javabase.Address
                                                  WHERE javabase.Address.postcode = 'AB12 1XY';
INSERT INTO javabase.Person (username, addressId) SELECT
                                                    'user-3',
                                                    id
                                                  FROM javabase.Address
                                                  WHERE javabase.Address.postcode = 'AB12 1XY';
INSERT INTO javabase.Person (username, addressId) SELECT
                                                    'user-4',
                                                    id
                                                  FROM javabase.Address
                                                  WHERE javabase.Address.postcode = 'XY98 9BA';
INSERT INTO javabase.Person (username, addressId) SELECT
                                                    'user-5',
                                                    id
                                                  FROM javabase.Address
                                                  WHERE javabase.Address.postcode = 'XY98 9BA';
INSERT INTO javabase.Person (username, addressId) SELECT
                                                    'user-6',
                                                    id
                                                  FROM javabase.Address
                                                  WHERE javabase.Address.postcode = 'XY98 9BA';
INSERT INTO javabase.Person (username, addressId) SELECT
                                                    'user-7',
                                                    id
                                                  FROM javabase.Address
                                                  WHERE javabase.Address.postcode = 'ZZ1 0MN';
INSERT INTO javabase.Person (username, addressId) SELECT
                                                    'user-8',
                                                    id
                                                  FROM javabase.Address
                                                  WHERE javabase.Address.postcode = 'ZZ1 0MN';
INSERT INTO javabase.Person (username, addressId) SELECT
                                                    'user-9',
                                                    id
                                                  FROM javabase.Address
                                                  WHERE javabase.Address.postcode = 'ZZ1 0MN';

INSERT INTO javabase.Numeric_Table (id, tinyint_field, smallint_field, mediumint_field, bigint_field,
                                    float_field, double_field, decimal_field)
VALUES (2, 1, 123, 123, 123, 123.2, 12323434.45, 18.10);

INSERT INTO javabase.String_Table (char_field, varchar_field, text_field, blob_field, tinytext_field, tinyblob_field,
                                   mediumtext_field, mediumblob_field, longtext_field, longblob_field,
                                   enum_field, numericId)
  SELECT
    'char-field',
    'varchar-field',
    'text_field',
    'blob_field',
    'tinytext_field',
    'tinyblob_field',
    'mediumtext_field',
    'mediumblob_field',
    'longtext_field',
    'longblob_field',
    'val-1',
    id
  FROM javabase.Numeric_Table
  WHERE javabase.Numeric_Table.id = 2;

INSERT INTO javabase.Date_Table (date_field, datetime_field, timestamp_field, time_field, year_field, numericId)
  SELECT
    '1988-01-23',
    '2038-01-19 03:14:07.123456',
    '1989-01-23',
    '22:34:35',
    1987,
    id
  FROM javabase.Numeric_Table
  WHERE javabase.Numeric_Table.id = 2;

INSERT INTO javabase.Author (first_name, last_name, age) VALUES ('Abraham', 'Silberschatz', 45);
INSERT INTO javabase.Author (first_name, last_name, age) VALUES ('Andrew', 'Tanenbaum', 56);
INSERT INTO javabase.Author (first_name, last_name, age) VALUES ('Raghu', 'Ramakrishnan', 32);

INSERT INTO javabase.Publisher (name) VALUES ('O\'Reilly');
INSERT INTO javabase.Publisher (name) VALUES ('Pearson');

INSERT INTO javabase.Author_Publisher (publisherId, author_first_name, author_last_name)
  SELECT
    id,
    'Abraham',
    'Silberschatz'
  FROM javabase.Publisher
  WHERE name = 'O\'Reilly';

INSERT INTO javabase.Author_Publisher (publisherId, author_first_name, author_last_name)
  SELECT
    id,
    'Andrew',
    'Tanenbaum'
  FROM javabase.Publisher
  WHERE name = 'Pearson';

INSERT INTO javabase.Book (name, author_first_name, author_last_name)
VALUES ('Database System Concepts', 'Abraham', 'Silberschatz');
INSERT INTO javabase.Book (name, author_first_name, author_last_name)
VALUES ('Computer Networks', 'Andrew', 'Tanenbaum');
INSERT INTO javabase.Book (name, author_first_name, author_last_name, reference_book_id)
  SELECT
    'Database Management Systems',
    'Raghu',
    'Ramakrishnan',
    id
  FROM javabase.Book
  WHERE author_first_name = 'Abraham';

INSERT INTO javabase.Student (username) VALUES ('jim');
INSERT INTO javabase.Student (username) VALUES ('mark');
INSERT INTO javabase.Student (username) VALUES ('eve');

INSERT INTO javabase.Course (name) VALUES ('Maths');
INSERT INTO javabase.Course (name) VALUES ('Science');
INSERT INTO javabase.Course (name) VALUES ('English');
INSERT INTO javabase.Course (name) VALUES ('Theology');

INSERT INTO javabase.Student_Course (studentId, courseId, credits)
  SELECT
    s.id,
    c.id,
    1
  FROM javabase.Student s, javabase.Course c
  WHERE s.username = 'jim' AND c.name = 'Maths';

INSERT INTO javabase.Student_Course (studentId, courseId, credits)
  SELECT
    s.id,
    c.id,
    2
  FROM javabase.Student s, javabase.Course c
  WHERE s.username = 'jim' AND c.name = 'Science';

INSERT INTO javabase.Student_Course (studentId, courseId, credits)
  SELECT
    s.id,
    c.id,
    3
  FROM javabase.Student s, javabase.Course c
  WHERE s.username = 'mark' AND c.name = 'Maths';

INSERT INTO javabase.Student_Course (studentId, courseId, credits)
  SELECT
    s.id,
    c.id,
    4
  FROM javabase.Student s, javabase.Course c
  WHERE s.username = 'mark' AND c.name = 'English';

INSERT INTO javabase.Team (name, teamMember1Id, teamMember2Id, teamMember3Id)
  SELECT
    'Rassilon',
    s1.id,
    s2.id,
    s3.id
  FROM javabase.Student s1, javabase.Student s2, javabase.Student s3
  WHERE s1.username = 'jim' AND s2.username = 'mark' AND s3.username = 'eve';
