## 基于物理引力的路网聚合及提取算法
程序算法计算流程：  
process:
1. 输入参数为：起止日期，公司缩写，车牌号。从ty数据中选取某线路的GPS轨迹数据，录入表gps_route。
2. 对数据进行一定的压缩，当相邻两个点行驶方向角小于R=5°时，保留距离当前点至少maxDist=10米左右的点，得到表comp_gps。
3. 计算r_num，当时间间隔超过rNumTime=300秒，则切分，标记趟数。
4. 以comp_gps为基础，计算每个GPS点的相似点，输出表gps_rel。
5. 压缩后的comp_gps，根据相似度点的个数小于一定的数进行剔除。最后得到表comp_gps2

loopCal:
6. 循环迭代计算，最终得到表process_gps，默认计算30次，每一次的结果都持久化到表process_gps中，按flag字段分区，得到flag=31的数据为最终的聚合轨迹数据。

lastStep:
7. 取flag=31的数据，抽取其中一条跑完全程的r_num轨迹，大约2000个GPS点。首先做去锯齿点计算，再做DP压缩
8. 关联上车牌号car_no的所属的32位corp_id和line_id信息，制作线路表。最终输出到hive中的表bigdata_test.t_mode_lineinfo和oracle中的t_mode_lineinfo
   

程序执行流程：
1. start-road-process.sh startDate endDate corp_short_name car_no  
例：start-road-process.sh 2021-03-22 2021-03-24 fjgz 闽FY6811
2. start-road-loopcal.sh
3. start-road-laststep.sh