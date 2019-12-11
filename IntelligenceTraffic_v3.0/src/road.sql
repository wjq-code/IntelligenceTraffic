SELECT area_name,road_id,COUNT (*) car_count,group_concat_distinct(monitor_id) monitor_infos FROM tmp_car_flow_basic
GROUP  BY arae_name,road_id
#使用开窗函数  获取每一个区域的topN路段：
select
area_name,
road_id,
car_count,
monitor_infos,
CASE
  where car_count > 170  THEN 'A LEVEL'
  where car_count > 170 AND car_count <= 170 THEN 'B LEVEL'
  where car_count > 150 AND car_count <= 160 THEN 'C LEVEL'
  ELSE 'D LEVEL'
END flow_Level
from (SELECT
area_name,
road_id,
car_count,
monitor_infos,
rank()over(partition by area_name order by car_count desc) rn
FROM tmp_area_road_flow_count) tmp
where rn <= 3