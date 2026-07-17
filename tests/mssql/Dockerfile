# Use the correct Microsoft Container Registry path
FROM ://microsoft.com

# The rest of your steps...
WORKDIR /install
COPY SQLEXPR_x64_ENU.exe .
RUN .\SQLEXPR_x64_ENU.exe /QS /ACTION=Install /FEATURES=SQL /INSTANCENAME=MSSQLSERVER /SQLSVCACCOUNT="NT AUTHORITY\System" /SQLSYSADMINACCOUNTS="BUILTIN\ADMINISTRATORS" /AGREETOLICENSE /IACCEPTSQLSERVERLICENSETERMS
EXPOSE 1433
CMD [ "powershell", "Start-Service MSSQLSERVER; while ($true) { Start-Sleep -Seconds 3600 }" ]