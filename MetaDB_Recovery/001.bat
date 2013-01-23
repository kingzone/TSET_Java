@echo off
bteq < 001.txt
if exist 001.out (
  rem get lines from the 3rd line
  set n=1
  SetLocal EnableDelayedExpansion
  for /f "delims=" %%i in (001.out) do (
    echo %%i
    if !n! geq 3 (
      rem echo %%i >> 001.o
	  echo %%i >> PhysicalConfig.out
    ) else (
      echo not matching...
    )
    set /a n=!n!+1
  )
  endlocal
) else (
  echo not found 001.out
)

bteq < 002.txt
if exist 002.out (
  rem get lines from the 3rd line
  set n=1
  SetLocal EnableDelayedExpansion
  for /f "delims=" %%i in (002.out) do (
    echo %%i
    if !n! geq 3 (
      rem echo %%i >> 002.o
	  echo %%i >> virtualConfig.out
    ) else (
      echo not matching...
    )
    set /a n=!n!+1
  )
  endlocal
) else (
  echo not found 002.out
)

bteq < 003.txt
if exist 003.out (
  rem get lines from the 3rd line
  set n=1
  SetLocal EnableDelayedExpansion
  for /f "delims=" %%i in (003.out) do (
    echo %%i
    if !n! geq 3 (
      rem echo %%i >> 003.o
	  echo %%i >> Opt_RAS.out
    ) else (
      echo not matching...
    )
    set /a n=!n!+1
  )
  endlocal
) else (
  echo not found 003.out
)

bteq < 004.txt
if exist 004.out (
  rem get lines from the 3rd line
  set n=1
  SetLocal EnableDelayedExpansion
  for /f "delims=" %%i in (004.out) do (
    echo %%i
    if !n! geq 3 (
      rem echo %%i >> 004.o
	  echo %%i >> Opt_Cost.out
    ) else (
      echo not matching...
    )
    set /a n=!n!+1
  )
  endlocal
) else (
  echo not found 004.out
)

bteq < 005.txt
if exist 005.out (
  rem get lines from the 3rd line
  set n=1
  SetLocal EnableDelayedExpansion
  for /f "delims=" %%i in (005.out) do (
    echo %%i
    if !n! geq 3 (
      rem echo %%i >> 005.o
	  echo %%i >> Opt_DBSCtl.out
    ) else (
      echo not matching...
    )
    set /a n=!n!+1
  )
  endlocal
) else (
  echo not found 005.out
)

bteq < 006.txt
if exist 006.out (
  rem get lines from the 3rd line
  set n=1
  SetLocal EnableDelayedExpansion
  for /f "delims=" %%i in (006.out) do (
    echo %%i
    if !n! geq 3 (
      rem echo %%i >> 006.o
	  echo %%i >> CostProfileTypes.out
    ) else (
      echo not matching...
    )
    set /a n=!n!+1
  )
  endlocal
) else (
  echo not found 006.out
)

bteq < 007.txt
if exist 007.out (
  rem get lines from the 3rd line
  set n=1
  SetLocal EnableDelayedExpansion
  for /f "delims=" %%i in (007.out) do (
    echo %%i
    if !n! geq 3 (
      rem echo %%i >> 007.o
	  echo %%i >> CostProfiles.out
    ) else (
      echo not matching...
    )
    set /a n=!n!+1
  )
  endlocal
) else (
  echo not found 007.out
)

bteq < 008.txt
if exist 008.out (
  rem get lines from the 3rd line
  set n=1
  SetLocal EnableDelayedExpansion
  for /f "delims=" %%i in (008.out) do (
    echo %%i
    if !n! geq 3 (
      rem echo %%i >> 008.o
	  echo %%i >> CostProfiles_2.out
    ) else (
      echo not matching...
    )
    set /a n=!n!+1
  )
  endlocal
) else (
  echo not found 008.out
)

bteq < 009.txt
if exist 009.out (
  rem get lines from the 3rd line
  set n=1
  SetLocal EnableDelayedExpansion
  for /f "delims=" %%i in (009.out) do (
    echo %%i
    if !n! geq 3 (
      rem echo %%i >> 009.o
	  echo %%i >> ConstantDefs.out
    ) else (
      echo not matching...
    )
    set /a n=!n!+1
  )
  endlocal
) else (
  echo not found 009.out
)

bteq < 010.txt
if exist 010.out (
  rem get lines from the 3rd line
  set n=1
  SetLocal EnableDelayedExpansion
  for /f "delims=" %%i in (010.out) do (
    echo %%i
    if !n! geq 3 (
      rem echo %%i >> 010.o
	  echo %%i >> ConstantValues.out
    ) else (
      echo not matching...
    )
    set /a n=!n!+1
  )
  endlocal
) else (
  echo not found 010.out
)

rem cleaning the directory ...
del 001.out
del 002.out
del 003.out
del 004.out
del 005.out
del 006.out
del 007.out
del 008.out
del 009.out
del 010.out

echo Done!