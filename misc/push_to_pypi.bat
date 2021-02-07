del /s /f /q c:\build\*.*
for /f %%f in ('dir /ad /b .\build\') do rd /s /q .\build\%%f

del /s /f /q .\dist\*.*
for /f %%f in ('dir /ad /b .\dist\') do rd /s /q .\dist\%%f

del /s /f /q .\fkbutils.egg-info\*.*
for /f %%f in ('dir /ad /b .\fkbutils.egg-info\') do rd /s /q .\fkbutils.egg-info\%%f

pip3 install twine
python setup.py sdist bdist_wheel
python -m twine upload dist/*

