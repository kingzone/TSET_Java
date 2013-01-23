@echo off
echo Now Retrieving all the tablenames and tablekinds...
bteq < 000.txt
echo Now creating show table/view statements...
if exist objs.out (
  echo .logon 153.64.80.9/dbc,dbc >> show.sql
  echo database hadoop; >> show.sql
  echo .export report file=show.out >> show.sql

  echo .set format off >> show.sql
  echo .set width 80 >> show.sql
  rem get lines from the 3rd line
  rem set n=1
  
  SetLocal EnableDelayedExpansion
  for /f "skip=2 delims=" %%i in (objs.out) do (
    
    rem if !n! geq 3 (
      endlocal
	  set ii=%%i
	  setlocal enabledelayedexpansion
	  if !ii:~-1! == T (
	    echo show table hadoop."!ii:~0,30!"; >> show.sql
		echo show stats values on hadoop."!ii:~0,30!"; >> show.sql
	  ) else (
	    if !ii:~-1! == V (
	      echo show view hadoop."!ii:~0,30!"; >> show.sql
	    ) else (
	      if !ii:~-1! == O (
		    echo show table hadoop."!ii:~0,30!"; >> show.sql
		  )
	    )
	  )
	  
    rem ) else (
    rem   echo not matching...
    rem )
    rem set /a n=!n!+1
  )
  echo write into show.sql SUCCESS.
  endlocal
  echo .export reset >> show.sql
  echo .logoff >> show.sql
  echo .exit >> show.sql
  
) else (
  echo not found objs.out
)

echo Now show tables and views et....
bteq < show.sql > show.log
rem if errorlevel 1 pause&exit
if errorlevel 1 echo sth wrong with the bteq command, but continue...
rem if errorlevel 1 pause

Rem matching and extracting the create table statements
if exist show.out (
  echo now extracting create table statements, waiting...5 minutes or so
  rem set myflag=false
  SetLocal EnableDelayedExpansion
  for /f "delims=" %%i in (show.out) do (
    endlocal
	echo %%i >> DDL.out
	set myflag=false
	echo %%i|findstr /i "\----------">nul && set myflag=true
	rem echo %%i|findstr /i "collect">nul && set myflag=true
	setlocal enabledelayedexpansion
	
    if "!myflag!" == "true" (
	  echo ; >> DDL.out
    ) 

  )
  endlocal
) else (
  echo not found show.out
)
ren DDL.out DDL.sql
Rem deleting the unused file
echo Now cleaning the unused files...
del objs.out
del show.sql
del show.out
del show.log
echo Done.
