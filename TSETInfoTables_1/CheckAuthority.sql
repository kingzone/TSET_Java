SEL 

TRIM(username) 

,TRIM(databasename)

,TRIM(tablename)

,'GRANT '|| CASE 

WHEN AccessRight = 'AF ' THEN 'ALTER FUNCTION' 

WHEN AccessRight = 'AP ' THEN 'ALTER PROCEDURE' 

WHEN AccessRight = 'AS ' THEN 'ABORT SESSION' 

WHEN AccessRight = 'CD ' THEN 'CREATE DATABASE' 

WHEN AccessRight = 'CF ' THEN 'CREATE FUNCTION' 

WHEN AccessRight = 'CG ' THEN 'CREATE TRIGGER' 

WHEN AccessRight = 'CM ' THEN 'CREATE MACRO' 

WHEN AccessRight = 'CO ' THEN 'CREATE PROFILE' 

WHEN AccessRight = 'CP ' THEN 'CHECKPOINT' 

WHEN AccessRight = 'CR ' THEN 'CREATE ROLE' 

WHEN AccessRight = 'CT ' THEN 'CREATE TABLE' 

WHEN AccessRight = 'CU ' THEN 'CREATE USER' 

WHEN AccessRight = 'CV ' THEN 'CREATE VIEW' 

WHEN AccessRight = 'D ' THEN 'DELETE' 

WHEN AccessRight = 'DD ' THEN 'DROP DATABASE' 

WHEN AccessRight = 'DF ' THEN 'DROP FUNCTION' 

WHEN AccessRight = 'DG ' THEN 'DROP TRIGGER' 

WHEN AccessRight = 'DM ' THEN 'DROP MACRO' 

WHEN AccessRight = 'DO ' THEN 'DROP PROFILE' 

WHEN AccessRight = 'DP ' THEN 'DUMP' 

WHEN AccessRight = 'DR ' THEN 'DROP ROLE' 

WHEN AccessRight = 'DT ' THEN 'DROP TABLE' 

WHEN AccessRight = 'DU ' THEN 'DROP USER' 

WHEN AccessRight = 'DV ' THEN 'DROP VIEW' 

WHEN AccessRight = 'E ' THEN 'EXECUTE' 

WHEN AccessRight = 'EF ' THEN 'EXECUTE FUNCTION' 

WHEN AccessRight = 'I ' THEN 'INSERT' 

WHEN AccessRight = 'IX ' THEN 'INDEX' 

WHEN AccessRight = 'MR ' THEN 'MONITOR RESOURCE' 

WHEN AccessRight = 'MS ' THEN 'MONITOR SESSION' 

WHEN AccessRight = 'PC ' THEN 'CREATE PROCEDURE' 

WHEN AccessRight = 'PD ' THEN 'DROP PROCEDURE' 

WHEN AccessRight = 'PE ' THEN 'EXECUTE PROCEDURE' 

WHEN AccessRight = 'RO ' THEN 'REPLICATION OVERRIDE' 

WHEN AccessRight = 'R ' THEN 'RETRIEVE/SELECT' 

WHEN AccessRight = 'RF ' THEN 'REFERENCE' 

WHEN AccessRight = 'RS ' THEN 'RESTORE' 

WHEN AccessRight = 'SS ' THEN 'SET SESSION RATE' 

WHEN AccessRight = 'SR ' THEN 'SET RESOURCE RATE' 

WHEN AccessRight = 'U ' THEN 'UPDATE' 

END || ' ON '||TRIM(databasename)||'.'||TRIM(tablename)||' to '||TRIM(username)||';' AS Permission

FROM dbc.AllRights

WHERE DatabaseName = 'DBNAME' and USERNAME <> 'LOGGEDINUSERNAME' AND TABLENAME <> 'All';