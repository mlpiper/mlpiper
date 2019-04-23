# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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