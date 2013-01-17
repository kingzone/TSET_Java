@echo off

rem produce drop sql statement, execute in 201.bat

set starttime=%time%

echo Now Retrieving all the tablenames and tablekinds order by createtimestamp desc...
bteq < drop.txt

if exist objs.out (
  rem echo .logon 153.64.80.9/dbc,dbc >> drop.sql
  rem echo database hadoop; >> drop.sql
  
  SetLocal EnableDelayedExpansion
  for /f "skip=2 delims=" %%i in (objs.out) do (
      
      endlocal
	  set ii=%%i
	  rem set iii=%ii:!=^^i%
	  
	  SetLocal EnableDelayedExpansion
	  echo !ii!
	  rem echo !ii:~-1!
	  if !ii:~-1! == T (
	    echo drop table hadoop."!ii:~0,30!"; >> drop.sql
		rem echo show stats values on hadoop."%%i"; >> drop.sql
	  ) else (
	    if !ii:~-1! == V (
	      echo drop view hadoop."!ii:~0,30!"; >> drop.sql
	    ) else (
	      if !ii:~-1! == O (
		    echo drop table hadoop."!ii:~0,30!"; >> drop.sql
		  )
	    )
	  )
	
  )
  echo write into drop.sql SUCCESS.
  endlocal
  rem echo .logoff >> drop.sql
  rem echo .exit >> drop.sql
  
) else (
  echo not found objs.out
)

Rem deleting the unused file
echo Now cleaning the unused files...
del objs.out

rem NO space around the =
set endtime=%time%

echo starttime: %starttime%; endtime: %endtime%; time elapsed: %endtime%-%starttime%

echo Done.