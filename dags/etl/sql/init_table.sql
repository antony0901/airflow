CREATE TABLE IF NOT EXISTS `CompletionRecord` (
  `Created` datetime(6) NOT NULL,
  `Updated` datetime(6) NOT NULL,
  `Id` char(36) COLLATE utf8_unicode_ci NOT NULL,
  `Version` int(11) NOT NULL,
  `CompletionDate` datetime(6) NOT NULL,
  `LearnerId` char(36) COLLATE utf8_unicode_ci NOT NULL,
  `LookupShortCourse` bit(1) NOT NULL DEFAULT b'0',
  `SecondaryLearnerId` char(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `ConvertedFromV1` bit(1) NOT NULL DEFAULT b'0',
  `IsActive` bit(1) NOT NULL DEFAULT b'1',
  PRIMARY KEY (`Id`),
  KEY `LearnerId` (`LearnerId`),
  KEY `CompletionDate` (`CompletionDate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
