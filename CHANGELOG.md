# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [UNRELEASED] - YYYY-MM-DD

### Added
- Add support for Spark 3.2 and Scala 2.13
- Support to ignore columns in diff API (#63)

### Removed
- Removed support for Spark 2.4

## [1.3.3] - 2020-12-17

### Added
- Add support for Spark 3.1

## [1.3.2] - 2020-12-17

### Changed
- Refine conditional transformation helper methods

## [1.3.1] - 2020-12-10

### Changed
- Refine conditional transformation helper methods

## [1.3.0] - 2020-12-07

### Added
- Add transformation to compute histogram (#26)
- Add conditional transformation helper methods (#27)
- Add partitioned writing helpers that simplifies writing optimally ordered partitioned data (#29)

## [1.2.0] - 2020-10-06

### Added
- Add diff modes (#22): column-by-column, side-by-side, left and right side diff modes
- Adds sparse mode (#23): diff DataFrame contains only changed values

## [1.1.0] - 2020-08-24

### Added
- Add Python API for Diff transformation.
- Add change column to Diff transformation providing column names of all changed columns in a row.
- Add fluent methods to change immutable diff options.
- Add `backticks` method to handle column names that contain dots (`.`).

## [1.0.0] - 2020-03-12

Initial release of the project

### Added
- Add Diff transformation for Datasets.
