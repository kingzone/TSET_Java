@echo off
rem construct the sql statements of 001 PhysicalConfig
if exist PhysicalConfig.out (
  for /f "tokens=1-7,*" %%i in (PhysicalConfig.out) do (
    echo insert into hadoop
  )
) else (
  echo can NOT find PhysicalConfig.out
)

rem construct the sql statements of 006 DBC.CostProfileTypes
echo construct the sql statements of 006 ...
if exist CostProfileTypes.out (
  for /f "tokens=1,2,* delims= " %%i in (CostProfileTypes.out) do (
    echo insert into DBC.CostProfileTypes values (%%i, '%%j', '%%k'^); >> 106.sql
  )
) else (
  echo can NOT find CostProfileTypes.out
)

rem construct the sql statements of 007 DBC.CostProfiles
echo construct the sql statements of 007 ...
if exist CostProfiles.out (
  for /f "tokens=1-4,* delims= " %%i in (CostProfiles.out) do (
    echo insert into DBC.CostProfiles values (%%i, '%%j', %%k, '%%l', '%%m'^); >> 107.sql
  )
) else (
  echo can NOT find CostProfiles.out
)

rem construct the sql statements of 008 DBC.CostProfiles
rem the same with 107.sql
if exist CostProfiles.out (
  for /f "tokens=1-4,*" %%i in (CostProfiles.out) do (
    rem echo insert into DBC.CostProfiles values (%%i, '%%j', %%k, '%%l', '%%m'^); >> 108.sql
	echo the same with 107.sql...
  )
) else (
  echo can NOT find CostProfiles.out
)

rem construct the sql statements of 009 DBC.ConstantDefs
echo construct the sql statements of 009 ...
if exist ConstantDefs.out (
  SetLocal EnableDelayedExpansion
  for /f "tokens=1-6,*" %%i in (ConstantDefs.out) do (
    rem Be Careful. The 2nd and fifth columns(that is, %%j and %%m here) is FLOAT type, but may have commas, so we need ''
	rem also, the 5th column may have operator ?, conver ? to 0.0
	set mm=%%m
	set mm=!mm:?=0!
	echo mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm !mm!
	rem also, the last column(namely, %%o) may contain symbol ', replace it with "
	set oo=%%o
	set oo=!oo:'="!
	echo ooooooooooooooooooooooooooooooooo !oo!
    echo insert into DBC.ConstantDefs values (%%i, '%%j', '%%k', '%%l', '!mm!', '%%n', '!oo!'^); >> 109.sql
	if errorlevel 1 (
	  echo ERROR when construct the sql statements of DBC.ConstantDefs
	  pause & exit
	)
  )
  endlocal
) else (
  echo can NOT find ConstantDefs.out
)

rem construct the sql statements of 010 DBC.ConstantValues
echo construct the sql statements of 010 ...
if exist ConstantValues.out (
  for /f "tokens=1-4,*" %%i in (ConstantValues.out) do (
    rem Be Careful. The 3rd and 4th columns(that is, %%k and %%l here) is INTEGER,FLOAT type, but may have commas, so we need ''
    echo insert into DBC.ConstantValues values (%%i, '%%j', '%%k', '%%l', '%%m'^); >> 110.sql
	if errorlevel 1 (
	  echo ERROR when construct the sql statements of DBC.ConstantValues
	  pause & exit
	)
  )
) else (
  echo can NOT find ConstantValues.out
)

rem rename the *.sql files 
echo Now rename the *.sql files 
ren 106.sql CostProfileTypes.sql
ren 107.sql CostProfiles.sql
ren 109.sql ConstantDefs.sql
ren 110.sql ConstantValues.sql
echo Done.
