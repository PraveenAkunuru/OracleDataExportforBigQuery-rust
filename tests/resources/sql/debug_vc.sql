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

set echo on
set feedback on
set heading on
set pagesize 200
SELECT count(*) as CNT FROM all_tab_columns WHERE owner = 'TESTUSER' AND table_name = 'TEST_VIRTUAL_COLS';
SELECT column_name, virtual_column FROM all_tab_columns WHERE owner = 'TESTUSER' AND table_name = 'TEST_VIRTUAL_COLS';
EXIT
