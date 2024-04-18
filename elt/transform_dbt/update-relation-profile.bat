@echo off
set "relation_name=%1"
set "schema=%2"

if "%relation_name%"=="" (
    echo usage: update-relation-profile.bat RELATION_NAME SCHEMA
    exit /b 1
)

if "%schema%"=="" (
    echo usage: update-relation-profile.bat RELATION_NAME SCHEMA
    exit /b 1
)

dbt run-operation print_profile_docs --args "{\"schema\": \"%schema%\", \"relation_name\": \"%relation_name%\", \"docs_name\": \"dbt_profiler_results__%schema%_%relation_name%\"}" > temp.txt

set "PROFILE="
for /f "delims=" %%a in (temp.txt) do (
    echo %%a | findstr /r /c:"{%.docs.*%}" >nul && set "flag=1"
    if defined flag echo %%a
    echo %%a | findstr /r /c:"{%.enddocs.%}" >nul && set "flag="
)

set "output_dir=docs\dbt_profiler\%schema%"
set "output_path=%output_dir%\%relation_name%.md"
mkdir "%output_dir%" 2>nul
type NUL >"%output_path%"

for /f "delims=" %%b in (temp.txt) do (
    echo %%b>>"%output_path%"
)

del temp.txt

echo %output_path%
type "%output_path%"
