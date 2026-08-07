# Table using SRID attributes and spatial indexes
#
# Version-gated comments ensure that we use the proper SRID attribute syntax
# for the flavor: MySQL uses "SRID %d", while MariaDB uses "REF_SYSTEM_ID=%d"

use testing;
CREATE TABLE has_geo (
	id int unsigned NOT NULL,
	geo1 geometry,
	geo2 geometry NOT NULL,
	geo3 geometry /*M! REF_SYSTEM_ID=4326 */ /*!80003 SRID 4326 */,
	geo4 geometry /*M! REF_SYSTEM_ID=4326 */ NOT NULL /*!80003 SRID 4326 */,
	geo5 geometry /*M! REF_SYSTEM_ID=0 */ /*!80003 SRID 0 */,
	geo6 geometry /*M! REF_SYSTEM_ID=0 */ NOT NULL /*!80003 SRID 0 */,
	SPATIAL INDEX s1 (geo2),
	SPATIAL INDEX s2 (geo4),
	PRIMARY KEY (id)
) ENGINE=InnoDB;