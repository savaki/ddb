# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.3] - 2025-10-10

### Fixed
- Fixed DynamoDB stream event JSON unmarshaling by adding custom `Image` type with `UnmarshalJSON` method
- Stream events can now be properly deserialized with `json.Unmarshal` into `ddb.Event`
- `Image` type (Keys, NewImage, OldImage) now works seamlessly with `attributevalue.UnmarshalMap`

### Added
- Comprehensive test data in `testdata/streams/` for INSERT, MODIFY, REMOVE events
- Tests verifying `Image` compatibility with AWS SDK's `attributevalue.UnmarshalMap`

## [2.0.2] - 2025-10-09

### Changed
- Simplified WithTable API by removing error return from Setup

### Added
- Add ddbtest.WithTable helper and documentation

## [2.0.1] - 2025-10-09

### Changed
- Update v2 README with performance highlights

### Added
- Optimize expression assembly performance (20-60% faster)
- Add comprehensive benchmarks for expression assembly

## [2.0.0] - 2025-10-09

### Changed
- Complete migration to AWS SDK v2
- Remove all AWS SDK v1 references

### Added
- Add v1 to v2 upgrade guide to README
- Add coverage tests and update billing mode type in DDL

[2.0.3]: https://github.com/savaki/ddb/compare/v2.0.2...v2.0.3
[2.0.2]: https://github.com/savaki/ddb/compare/v2.0.1...v2.0.2
[2.0.1]: https://github.com/savaki/ddb/compare/v2.0.0...v2.0.1
[2.0.0]: https://github.com/savaki/ddb/releases/tag/v2.0.0
