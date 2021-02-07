from setuptools import setup

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(name='fkbutils',
      version="0.1.1",
      description='Utility functions that I need often across multiple projects!',
      install_requires=required,
      include_package_data=True,
      zip_safe=False,
      author="Felix Kleine Bösing",
      author_email="felix.boesing@t-online.de",
      packages=["fkbutils"])
