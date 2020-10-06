# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

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
