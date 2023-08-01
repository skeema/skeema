# Table using SRID attributes and spatial indexes
#
# Version-gated comments ensure that we use the proper SRID attribute syntax
# for the flavor: MySQL 8.0+ uses "SRID %d", while MariaDB 10.1+ uses
# "REF_SYSTEM_ID=%d"
#
# Similarly we only create spatial indexes on flavors supporting them in
# InnoDB: MySQL 5.7+ or MariaDB 10.2+.

use testing;
CREATE TABLE has_geo (
	id int unsigned NOT NULL,
	geo1 geometry,
	geo2 geometry NOT NULL,
	geo3 geometry /*M! REF_SYSTEM_ID=4326 */ /*!80003 SRID 4326 */,
	geo4 geometry /*M! REF_SYSTEM_ID=4326 */ NOT NULL /*!80003 SRID 4326 */,
	geo5 geometry /*M! REF_SYSTEM_ID=0 */ /*!80003 SRID 0 */,
	geo6 geometry /*M! REF_SYSTEM_ID=0 */ NOT NULL /*!80003 SRID 0 */,
	/*!50700 SPATIAL INDEX s1 (geo2),*/ /*M!100200 SPATIAL INDEX s1 (geo2),*/
	/*!50700 SPATIAL INDEX s2 (geo4),*/ /*M!100200 SPATIAL INDEX s2 (geo4),*/
	PRIMARY KEY (id)
) ENGINE=InnoDB;