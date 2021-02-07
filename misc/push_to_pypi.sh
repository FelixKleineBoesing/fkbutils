rm -rf dist
rm -rf build
rm -rf fkbutils.egg-info

pip3 install twine
python setup.py sdist bdist_wheel
python -m twine upload dist/*
