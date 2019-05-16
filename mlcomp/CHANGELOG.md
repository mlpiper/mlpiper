# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.3.1] - [Unreleased]
### Added
- add ComponentInfo, ComponentArgumentInfo, ComponentConnectionInfo infrastructure
- add 'mlpiper wizard' tool to build components

## [1.2.2] - 2019-05-01
### Added
- Add support for rest model serving pipeline's execution from `mlpiper` command line tool
- Add quickstart section to readme

### Fixed
- Fix a graceful shutdown of pipeline's execution under Darwin OS 
- [REF-6108] explicitly specify utf-8 encoding when opening text file

## [1.2.1] - 2019-04-22
### Added
- include all Java standalone/connected component's jars into classpath
- add support for requirements.txt
- mlpiper: always copy comp json file regardless of name
- mlpiper: print deps for a given pipeline and components
- mlpiper: pass a mlcomp_jar as parameter

### Changed
- change engineType Python -> Generic
- check __init__.py file only for Python components

### Fixed
- copying files from shared comps dir to separate comp dir

### Security
### Removed
### Deprecated

