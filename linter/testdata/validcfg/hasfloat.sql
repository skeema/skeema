CREATE TABLE `hasfloat` (
  id int(10) unsigned NOT NULL,
  decimal_is_fine decimal(25,2),
  float_is_bad float(23), /* annotations: has-float */
  float2_is_bad float(7,4), /* annotations: has-float */
  double_is_bad double(53,2), /* annotations: has-float */
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;