@echo off
copy C:\githelper\Python.gitignore .gitignore 

echo "# konfuzed" >> README.md

git init



rem Get current dir 
rem for %I in (.) do set cur=%~nxI
for %%I in (.) do set cur=%%~nxI

rem Your personal access token should be here: C:\gitToken.txt
set /p tkn=<C:\gitToken.txt

echo Creating Git repository using access token 
curl -sk --header "Authorization: token %tkn%"  https://api.github.com/user/repos -d '{"name":"%cur%","private":false}' | jq --raw-output .html_url > gitURL.txt

set /p repolink=<gitURL.txt

echo %cur% >> README.md
echo %repolink% >> README.md

git add README.md

git commit -m "first commit"
git branch -M main

git remote add origin %repolink%

git push -u origin main

git push --set-upstream origin main

rem Now Add all Commit All.
git add --all
git commit -am "Changed file Commit."
git push -u origin main

set /p DUMMY=Hit ENTER to continue...
exit
