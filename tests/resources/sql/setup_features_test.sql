-- Copyright 2026 Google LLC
-- 
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
--      http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Setup Test Tables for New Features
-- Run this in Oracle (e.g., via sqlplus or SQLcl)

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE TEST_VIRTUAL_COLS PURGE';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE TEST_SPATIAL PURGE';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE TEST_INTERVALS PURGE';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

-- 1. Virtual Columns
CREATE TABLE TEST_VIRTUAL_COLS (
    ID NUMBER PRIMARY KEY,
    QUANTITY NUMBER,
    PRICE NUMBER,
    TOTAL_COST NUMBER GENERATED ALWAYS AS (QUANTITY * PRICE) VIRTUAL,
    DESC_UPPER VARCHAR2(100) GENERATED ALWAYS AS (UPPER('Item ' || ID)) VIRTUAL
);

INSERT INTO TEST_VIRTUAL_COLS (ID, QUANTITY, PRICE) VALUES (1, 10, 5.5);
INSERT INTO TEST_VIRTUAL_COLS (ID, QUANTITY, PRICE) VALUES (2, 20, 10);
COMMIT;

-- 2. Spatial Data (SDO_GEOMETRY)
CREATE TABLE TEST_SPATIAL (
    ID NUMBER PRIMARY KEY,
    NAME VARCHAR2(50),
    LOCATION SDO_GEOMETRY
);

INSERT INTO TEST_SPATIAL VALUES (
    1, 
    'Point A', 
    SDO_GEOMETRY(2001, 4326, SDO_POINT_TYPE(-122.4194, 37.7749, NULL), NULL, NULL)
);
COMMIT;

-- 3. Intervals
CREATE TABLE TEST_INTERVALS (
    ID NUMBER PRIMARY KEY,
    DURATION_YM INTERVAL YEAR(2) TO MONTH,
    DURATION_DS INTERVAL DAY(2) TO SECOND(6)
);

INSERT INTO TEST_INTERVALS VALUES (
    1,
    INTERVAL '1-2' YEAR TO MONTH,
    INTERVAL '3 12:30:45.123456' DAY TO SECOND
);
COMMIT;

-- 4. UROWID (If supported, usually it is)
-- Creating a table index-organized or external might trigger UROWID, but we can just use a column type
CREATE TABLE TEST_UROWID_TABLE (
    ID NUMBER PRIMARY KEY,
    REF_ID UROWID
);
-- Hard to insert arbitrary UROWID, usually comes from system.
-- We will skip explicit insert verification for UROWID content, just structure.

-- 5. BOOLEAN (Oracle 23c only - might fail on older XE)
-- We wrap in dynamic SQL to avoid breaking script on older DBs
BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE TEST_BOOLEAN (ID NUMBER PRIMARY KEY, IS_ACTIVE BOOLEAN)';
    EXECUTE IMMEDIATE 'INSERT INTO TEST_BOOLEAN VALUES (1, TRUE)';
    EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION
    WHEN OTHERS THEN 
        DBMS_OUTPUT.PUT_LINE('Skipping BOOLEAN test: ' || SQLERRM);
END;
/

EXIT;
