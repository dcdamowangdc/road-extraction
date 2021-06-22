-- hive中的表

-- ty中取到的一条线路的gps轨迹数据
CREATE TABLE `bigdata_test.gps_route`(
  `rcrd_date` string,
  `car_no` string,
  `up_dn` string,
  `rcrd_time` string,
  `can_speed` string,
  `can_mile` string,
  `lo_lgt` string,
  `lo_ltt` string,
  `lo_drc` string)

-- 将gps_route进行一定规则的压缩，得到表comp_gps
CREATE TABLE `bigdata_test.comp_gps`(
   rcrd_date string,
 car_no string,
 rcrd_time string,
 lo_lgt string,
 lo_ltt string,
 lo_drc string,
 r_num string)

-- 对comp_gps去除相似点个数小于7的gps点后得到表comp_gps2
CREATE TABLE `bigdata_test.comp_gps2`(
  `rcrd_date` string,
  `car_no` string,
  `rcrd_time` string,
  `lo_lgt` string,
  `lo_ltt` string,
  `lo_drc` string,
  `r_num` string)

-- gps_rel gps关系表，通过join得到每个gps点的相似点关系
   CREATE TABLE `bigdata_test.gps_rel`(
   `car_no` string,
   `rcrd_time` string,
   `drc0` string, -- 弧度
   `car_noa` string,
   `rcrd_timea` string,
   `dist15` string
   )

-- process表，是loopCal程序计算的表
   CREATE TABLE `bigdata_test.process_gps`(
  `rcrd_time` string,
  `lo_lgt` string,
  `lo_ltt` string)
PARTITIONED BY (
  `flag` string)

-- 结果表：
-- hive中保留的结果副本
create table bigdata_test.t_mod_lineinfo(
up_dn string,
road_id string,
start_lgt string,
start_ltt string,
end_lgt string,
end_ltt string,
start_lgt2 string,
start_ltt2 string,
end_lgt2 string,
end_ltt2 string
)
PARTITIONED BY (
  company string,
line_id string)


-- oracle中，测试库 CAN4_CS
--输出的结果表
   create table t_mod_lineinfo(
  company varchar2(32),
  line_id varchar2(32),
  up_dn varchar2(1),
  road_id varchar2(6),
  start_lgt number,
  start_ltt number,
  end_lgt number,
  end_ltt number,
  start_lgt2 number,
  start_ltt2 number,
  end_lgt2 number,
  end_ltt2 number,
  starttime date default sysdate not null
  )