set echo on
set feedback on
set heading on
set pagesize 200
SELECT count(*) as CNT FROM all_tab_columns WHERE owner = 'TESTUSER' AND table_name = 'TEST_VIRTUAL_COLS';
SELECT column_name, virtual_column FROM all_tab_columns WHERE owner = 'TESTUSER' AND table_name = 'TEST_VIRTUAL_COLS';
EXIT
