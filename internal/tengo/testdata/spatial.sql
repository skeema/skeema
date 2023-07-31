use testing;
CREATE TABLE has_geo (
	id int unsigned NOT NULL,
	geo1 geometry,
	geo2 geometry NOT NULL,
	geo3 geometry /*M! REF_SYSTEM_ID=4326 */ /*!80003 SRID 4326 */,
	geo4 geometry /*M! REF_SYSTEM_ID=4326 */ NOT NULL /*!80003 SRID 4326 */,
	geo5 geometry /*M! REF_SYSTEM_ID=0 */ /*!80003 SRID 0 */,
	geo6 geometry /*M! REF_SYSTEM_ID=0 */ NOT NULL /*!80003 SRID 0 */,
	PRIMARY KEY (id)
) ENGINE=InnoDB;