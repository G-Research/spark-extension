# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [2.11.0] - 2024-01-04

### Added

- Add count_null aggregate function (#206)
- Support reading parquet schema (#208)
- Add more columns to reading parquet metadata (#209, #211)
- Provide groupByKey shortcuts for groupBy.as (#213)
- Allow to install PIP packages into PySpark job (#215)
- Allow to install Poetry projects into PySpark job (#216)

## [2.10.0] - 2023-09-27

### Fixed

- Update setup.py to include parquet methods in python package (#191)

### Added

- Add --statistics option to diff app (#189)
- Add --filter option to diff app (#190)

## [2.9.0] - 2023-08-23

### Added

- Add key order sensitive map comparator (#187)

### Changed

- Use dataset encoder rather than implicit value encoder for implicit dataset extension class (#183)

### Fixed

- Fix key-sensitivity in map comparator (#186)

## [2.8.0] - 2023-05-24

### Added

- Add method to set and automatically unset Spark job description. (#172)
- Add column function that converts between .Net (C#, F#, Visual Basic) `DateTime.Ticks` and Spark timestamp / Unix epoch timestamps. (#153)

## [2.7.0] - 2023-05-05

### Added

- Spark app to diff files or tables and write result back to file or table. (#160)
- Add null value count to `parquetBlockColumns` and `parquet_block_columns`. (#162)
- Add `parallelism` argument to Parquet metadata methods. (#164)

### Changed

- Change data type of column name in `parquetBlockColumns` and `parquet_block_columns` to array of strings.
  Cast to string to get earlier behaviour (string column name). (#162)

## [2.6.0] - 2023-04-11

### Added

-  Add reader for parquet metadata. (#154)

## [2.5.0] - 2023-03-23

### Added

- Add whitespace agnostic diff comparator. (#137)
- Add Python whl package build. (#151)

## [2.4.0] - 2022-12-08

### Added

- Allow for custom diff equality. (#127)

### Fixed

- Fix Python API calling into Scala code. (#132)

## [2.3.0] - 2022-10-26

### Added

- Add diffWith to Scala, Java and Python Diff API. (#109)

### Changed

- Diff similar Datasets with ignoreColumns. Before, only similar DataFrame could be diffed with ignoreColumns. (#111)

### Fixed

- Cache before writing via partitionedBy to work around SPARK-40588. Unpersist via UnpersistHandle. (#124)

## [2.2.0] - 2022-07-21

### Added
- Add (global) row numbers transformation to Scala, Java and Python API. (#97)

### Removed
- Removed support for Pyton 3.6

## [2.1.0] - 2022-04-07

### Added
- Add sorted group methods to Dataset. (#76)

## [2.0.0] - 2021-10-29

### Added
- Add support for Spark 3.2 and Scala 2.13.
- Support to ignore columns in diff API. (#63)

### Removed
- Removed support for Spark 2.4.

## [1.3.3] - 2020-12-17

### Added
- Add support for Spark 3.1.

## [1.3.2] - 2020-12-17

### Changed
- Refine conditional transformation helper methods.

## [1.3.1] - 2020-12-10

### Changed
- Refine conditional transformation helper methods.

## [1.3.0] - 2020-12-07

### Added
- Add transformation to compute histogram. (#26)
- Add conditional transformation helper methods. (#27)
- Add partitioned writing helpers that simplifies writing optimally ordered partitioned data. (#29)

## [1.2.0] - 2020-10-06

### Added
- Add diff modes (#22): column-by-column, side-by-side, left and right side diff modes.
- Adds sparse mode (#23): diff DataFrame contains only changed values.

## [1.1.0] - 2020-08-24

### Added
- Add Python API for Diff transformation.
- Add change column to Diff transformation providing column names of all changed columns in a row.
- Add fluent methods to change immutable diff options.
- Add `backticks` method to handle column names that contain dots (`.`).

## [1.0.0] - 2020-03-12

### Added
- Add Diff transformation for Datasets.
