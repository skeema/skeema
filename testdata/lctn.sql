DROP DATABASE product;
DROP DATABASE analytics;

CREATE DATABASE NameCase;
USE NameCase;

CREATE TABLE `Users` (
  `ID` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `Name` varchar(30) NOT NULL,
  `Credits` decimal(9,2) DEFAULT '10.00',
  `LastModified` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `Name` (`Name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

CREATE TABLE `Posts` (
  `ID` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `UserID` bigint(20) unsigned NOT NULL,
  `Body` text,
  `CreatedAt` datetime /*!50601 DEFAULT CURRENT_TIMESTAMP*/,
  `EditedAt` datetime /*!50601 DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP*/,
  PRIMARY KEY (`ID`),
  KEY `UserCreated` (`UserID`,`CreatedAt`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

CREATE TABLE `Comments` (
  `ID` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `PostID` bigint(20) unsigned NOT NULL,
  `UserID` bigint(20) unsigned NOT NULL,
  `CreatedAt` datetime DEFAULT NULL,
  `Body` text,
  PRIMARY KEY (`ID`),
  KEY `User` (`UserID`),
  KEY `Post` (`PostID`),
  CONSTRAINT `UserFK` FOREIGN KEY (`UserID`) REFERENCES `Users` (`ID`),
  CONSTRAINT `PostFK` FOREIGN KEY (`PostID`) REFERENCES `Posts` (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

CREATE FUNCTION AmountOwed(Credits float) RETURNS float DETERMINISTIC RETURN IF(Credits < 0.0, Credits * -1.0, 0.0);

