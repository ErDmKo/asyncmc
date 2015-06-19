import os
import sys
from setuptools import setup, find_packages

version = '0.4'

def read(f):
    return open(os.path.join(os.path.dirname(__file__), f)).read().strip()

install_requires = [
    'tornado>=4.1',
    'toro>=0.8'
]

setup(name='asyncmc',
      version=version,
      description=('Minimal pure python tornado memcached client'),
      long_description='\n\n'.join((read('README.rst'), read('CHANGES.rst'))),
      classifiers=[
          'License :: OSI Approved :: MIT License',
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers',
          'Programming Language :: Python',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3',
          'Operating System :: POSIX',
          'Operating System :: MacOS :: MacOS X',
          'Operating System :: Microsoft :: Windows',
          'Environment :: Web Environment'],
      author='Eroshenko Dmitriy',
      author_email='erdmko@gmail.com',
      url='https://github.com/ErDmKo/asyncmc/',
      license='MIT',
      packages=find_packages(),
      install_requires = install_requires,
      include_package_data = True)
