# Changelog

## [1.1.0](https://github.com/googleapis/go-sql-spanner/compare/v1.0.1...v1.1.0) (2023-07-05)


### Features

* Add missing spanner config properties ([#152](https://github.com/googleapis/go-sql-spanner/issues/152)) ([c6bda23](https://github.com/googleapis/go-sql-spanner/commit/c6bda23e86f3679bb48c33e19ebf413ca984a4ee))
* **driver:** Replace `value.Value` with `Value()` return ([#139](https://github.com/googleapis/go-sql-spanner/issues/139)) ([6f2b96e](https://github.com/googleapis/go-sql-spanner/commit/6f2b96ea14d87a3edfdbaa0738139cd09862e618))

## [1.0.1](https://github.com/googleapis/go-sql-spanner/compare/v1.0.0...v1.0.1) (2023-03-06)


### Bug Fixes

* **deps:** Update module github.com/googleapis/go-sql-spanner to v1 ([#125](https://github.com/googleapis/go-sql-spanner/issues/125)) ([e1ba360](https://github.com/googleapis/go-sql-spanner/commit/e1ba360543b59ae930b4228a03b94cc724dd14d3))

## [1.0.0](https://github.com/googleapis/go-sql-spanner/compare/v1.0.0...v1.0.0) (2022-09-15)


### Features

* Add ARRAY support ([#19](https://github.com/googleapis/go-sql-spanner/issues/19)) ([6b1556a](https://github.com/googleapis/go-sql-spanner/commit/6b1556a8db409fbab0998fdcde59521b26495472))
* Add client side statement parser ([#38](https://github.com/googleapis/go-sql-spanner/issues/38)) ([969bf52](https://github.com/googleapis/go-sql-spanner/commit/969bf52b2cda303349746de9730557242082893c))
* Add support for JSON data type ([#39](https://github.com/googleapis/go-sql-spanner/issues/39)) ([ef52036](https://github.com/googleapis/go-sql-spanner/commit/ef5203657aa8e2173e387ea6aede02d457577790))
* Add support for stale reads ([#44](https://github.com/googleapis/go-sql-spanner/issues/44)) ([2e3a264](https://github.com/googleapis/go-sql-spanner/commit/2e3a2645073d7c9174b3aec934c1e1fcbb06534c))
* Add support of positional parameter in the queries ([#110](https://github.com/googleapis/go-sql-spanner/issues/110)) ([a71a457](https://github.com/googleapis/go-sql-spanner/commit/a71a457261ea8e522d320726a8aeea6768f08acf))
* Allow host in dsn and use statement based transactions ([#10](https://github.com/googleapis/go-sql-spanner/issues/10)) ([0528e13](https://github.com/googleapis/go-sql-spanner/commit/0528e13eed6ccb0b71636554f79c9d278242987c))
* Create standalone samples that run against emulator ([#30](https://github.com/googleapis/go-sql-spanner/issues/30)) ([22b127e](https://github.com/googleapis/go-sql-spanner/commit/22b127e111dc7f8e3a8dfb83e1f3dd736640fcaf))
* Support getting the commit timestamp of a transaction or statement ([#52](https://github.com/googleapis/go-sql-spanner/issues/52)) ([802e7be](https://github.com/googleapis/go-sql-spanner/commit/802e7be6dd18dd6c75991bc129116892a45de944))
* Support mutations ([#43](https://github.com/googleapis/go-sql-spanner/issues/43)) ([2d698b7](https://github.com/googleapis/go-sql-spanner/commit/2d698b754205888fcd4487ec0793c956f42bbf56))


### Bug Fixes

* Add ddl support and change tests to run ddl from driver ([259f98b](https://github.com/googleapis/go-sql-spanner/commit/259f98b017849d158ff799de5c947bb0c39eb4f2))
* Added ddl support to driver, changed tests to call driver rather than api directly ([a9c4c8a](https://github.com/googleapis/go-sql-spanner/commit/a9c4c8a50b3823d2eff368fd672dd7e4adfff1f5))
* Allow users to specify custom credentials ([#57](https://github.com/googleapis/go-sql-spanner/issues/57)) ([1715929](https://github.com/googleapis/go-sql-spanner/commit/171592955606f2b7ff3313d2ab6ddf17ea785f3b))
* Always set a value for dest in Next ([#34](https://github.com/googleapis/go-sql-spanner/issues/34)) ([7b8190c](https://github.com/googleapis/go-sql-spanner/commit/7b8190cbb3d63eac2f4b311208d1da7ea282436b))
* Check named value parameter types ([#35](https://github.com/googleapis/go-sql-spanner/issues/35)) ([f260dd2](https://github.com/googleapis/go-sql-spanner/commit/f260dd247f6a5c69d0d8ec1ac0fb7136f978ae05))
* **deps:** Update all modules ([#108](https://github.com/googleapis/go-sql-spanner/issues/108)) ([2d13f6d](https://github.com/googleapis/go-sql-spanner/commit/2d13f6dcc272d3354a1ebe001fc711e731540aca))
* **deps:** Update google.golang.org/genproto commit hash ([#78](https://github.com/googleapis/go-sql-spanner/issues/78)) ([c9ed2ac](https://github.com/googleapis/go-sql-spanner/commit/c9ed2ac088d9b2bd4cc3bcb613aa0595b1ef9b73))
* **deps:** Update google.golang.org/genproto commit hash to 1739428 ([#81](https://github.com/googleapis/go-sql-spanner/issues/81)) ([3f6ba94](https://github.com/googleapis/go-sql-spanner/commit/3f6ba948cc361edd4190392a629dbef764979ee2))
* **deps:** Update module cloud.google.com/go to v0.100.2 ([#71](https://github.com/googleapis/go-sql-spanner/issues/71)) ([cac55f0](https://github.com/googleapis/go-sql-spanner/commit/cac55f092744104371fa6e539928b2bf73fae1ab))
* **deps:** Update module cloud.google.com/go to v0.102.1 ([#103](https://github.com/googleapis/go-sql-spanner/issues/103)) ([23d315e](https://github.com/googleapis/go-sql-spanner/commit/23d315e644d740a77ff39ac6d1553db81229f2c7))
* **deps:** Update module cloud.google.com/go/spanner to v1.29.0 ([#74](https://github.com/googleapis/go-sql-spanner/issues/74)) ([9a676ba](https://github.com/googleapis/go-sql-spanner/commit/9a676bad33664faf2e6ce937a6c7393407545723))
* **deps:** Update module github.com/google/go-cmp to v0.5.7 ([#80](https://github.com/googleapis/go-sql-spanner/issues/80)) ([cae3a7a](https://github.com/googleapis/go-sql-spanner/commit/cae3a7a7e7ed3d0b9427ebf192a4ad55a7e08728))
* **deps:** Update module github.com/google/uuid to v1.3.0 ([#75](https://github.com/googleapis/go-sql-spanner/issues/75)) ([2072930](https://github.com/googleapis/go-sql-spanner/commit/2072930d8a8171d96a08c7be86578dca0b40b60a))
* **deps:** Update module google.golang.org/api to v0.68.0 ([#76](https://github.com/googleapis/go-sql-spanner/issues/76)) ([8af9417](https://github.com/googleapis/go-sql-spanner/commit/8af94172388cf28a2b8e9dd63e2bd7b40a262a0f))
* **deps:** Update module google.golang.org/grpc to v1.44.0 ([#82](https://github.com/googleapis/go-sql-spanner/issues/82)) ([7b20269](https://github.com/googleapis/go-sql-spanner/commit/7b2026924b9efbdb3ace243596ae978542f2b18b))
* Do not parse hints as parameters ([#45](https://github.com/googleapis/go-sql-spanner/issues/45)) ([56243a5](https://github.com/googleapis/go-sql-spanner/commit/56243a5a1169e86b3b14c02ab5c47a4b950a7f14))
* Pass userAgent in client config ([#118](https://github.com/googleapis/go-sql-spanner/issues/118)) ([2c97068](https://github.com/googleapis/go-sql-spanner/commit/2c97068e002c3c83acb9f181908cf2cbe025e516))
* Race condition when opening multiple connections in parallel as first action ([#59](https://github.com/googleapis/go-sql-spanner/issues/59)) ([0971f81](https://github.com/googleapis/go-sql-spanner/commit/0971f81129f36f519a4c1385a3cd634fa4492c3e))
* Refuse DDL during transactions ([#41](https://github.com/googleapis/go-sql-spanner/issues/41)) ([4e7fa97](https://github.com/googleapis/go-sql-spanner/commit/4e7fa97469752d1605b2af80997a03fd1005b5d9)), refs [#31](https://github.com/googleapis/go-sql-spanner/issues/31)
* Standardize returned errors ([#32](https://github.com/googleapis/go-sql-spanner/issues/32)) ([e780348](https://github.com/googleapis/go-sql-spanner/commit/e7803486f424d579c1b356cf7a500fbb62ac6040)), refs [#14](https://github.com/googleapis/go-sql-spanner/issues/14)
* Use correct type for decoding bytes ([49d08fc](https://github.com/googleapis/go-sql-spanner/commit/49d08fc7ade3559774c88a87b16d13d67c1eef57))


### Documentation

* Add comments to all samples to document what they do and how to use them ([#46](https://github.com/googleapis/go-sql-spanner/issues/46)) ([17a434f](https://github.com/googleapis/go-sql-spanner/commit/17a434f71d6d682ce7974b50f57e6a4193c4f892))
* Add DDL batch sample ([#48](https://github.com/googleapis/go-sql-spanner/issues/48)) ([82a23e4](https://github.com/googleapis/go-sql-spanner/commit/82a23e44db5752d4310133597183ee7967d0efea))
* Add documentation about contributing ([fd70120](https://github.com/googleapis/go-sql-spanner/commit/fd70120b979887d389633a8ffcb8fb647b163cbb))
* Add sample for all data types ([#51](https://github.com/googleapis/go-sql-spanner/issues/51)) ([5a0129b](https://github.com/googleapis/go-sql-spanner/commit/5a0129b45ea0a3b89d900024b99115523de8b8d7))
* Add sample for DML batches ([#49](https://github.com/googleapis/go-sql-spanner/issues/49)) ([bac4a4c](https://github.com/googleapis/go-sql-spanner/commit/bac4a4cef1f628918b925a1dc944ab7e3c732480))
* Add sample for PDML ([#53](https://github.com/googleapis/go-sql-spanner/issues/53)) ([9bd832b](https://github.com/googleapis/go-sql-spanner/commit/9bd832bec3a988c0b322a0563b7300b2c4e09e89))
* Add sample for read-only transaction ([#47](https://github.com/googleapis/go-sql-spanner/issues/47)) ([306c4ea](https://github.com/googleapis/go-sql-spanner/commit/306c4eae4dfef0f38d4c24177206c05092918ab5))
* Cleanup and extend readme ([#60](https://github.com/googleapis/go-sql-spanner/issues/60)) ([2d64f82](https://github.com/googleapis/go-sql-spanner/commit/2d64f827825255743ff4ea631c0b0e8913ef4148))
* Remove disclaimer from README ([#93](https://github.com/googleapis/go-sql-spanner/issues/93)) ([12780e5](https://github.com/googleapis/go-sql-spanner/commit/12780e57be1cfa3df753e92e3dd6c51e06dfb070))


### Miscellaneous Chores

* Release 1.0.0 ([#123](https://github.com/googleapis/go-sql-spanner/issues/123)) ([e7e0d8a](https://github.com/googleapis/go-sql-spanner/commit/e7e0d8a66d7f60cb6a32e28c29ab28ca4d62d5a5))
