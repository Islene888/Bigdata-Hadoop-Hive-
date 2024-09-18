create database db_msg ;
use db_msg;

--如果表已存在就删除
drop table if exists db_msg.tb_msg_source ;
--建表
create table db_msg.tb_msg_source(
msg_time string comment "消息发送时间",
sender_name string comment "发送人昵称",
sender_account string comment "发送人账号",
sender_sex string comment "发送人性别",
sender_ip string comment "发送人ip地址",
sender_os string comment "发送人操作系统",
sender_phonetype string comment "发送人手机型号",
sender_network string comment "发送人网络类型",
sender_gps string comment "发送人的GPS定位",
receiver_name string comment "接收人昵称",
receiver_ip string comment "接收人IP",
receiver_account string comment "接收人账号",
receiver_os string comment "接收人操作系统",
receiver_phonetype string comment "接收人手机型号",
receiver_network string comment "接收人网络类型",
receiver_gps string comment "接收人的GPS定位",
receiver_sex string comment "接收人性别",
msg_type string comment "消息类型",
distance string comment "双方距离",
message string comment "消息内容"
);

load data local inpath '/home/hadoop/chat_data-30W.csv' overwrite into table tb_msg_source;


select
    msg_time, sender_name, sender_ip, sender_phonetype, receiver_name, receiver_network
from tb_msg_source limit 10;





create table db_msg.tb_msg_etl(
msg_time string comment "消息发送时间",
sender_name string comment "发送人昵称",
sender_account string comment "发送人账号",
sender_sex string comment "发送人性别",
sender_ip string comment "发送人ip地址",
sender_os string comment "发送人操作系统",
sender_phonetype string comment "发送人手机型号",
sender_network string comment "发送人网络类型",
sender_gps string comment "发送人的GPS定位",
receiver_name string comment "接收人昵称",
receiver_ip string comment "接收人IP",
receiver_account string comment "接收人账号",
receiver_os string comment "接收人操作系统",
receiver_phonetype string comment "接收人手机型号",
receiver_network string comment "接收人网络类型",
receiver_gps string comment "接收人的GPS定位",
receiver_sex string comment "接收人性别",
msg_type string comment "消息类型",
distance string comment "双方距离",
message string comment "消息内容",
msg_day string comment "消息日",
msg_hour string comment "消息小时",
sender_lng double comment "经度",
sender_lat double comment "纬度"
);
--数据清洗，去除脏数据以及调整格式
INSERT OVERWRITE TABLE db_msg.tb_msg_etl
SELECT
    *,
    day(msg_time) as msg_day,
    HOUR(msg_time) as msg_hour,
    split(sender_gps, ',')[0] AS sender_lng,
    split(sender_gps, ',')[1] AS sender_lat
FROM tb_msg_source WHERE LENGTH(sender_gps) > 0;

--统计部分
--指标1：统计今日消息总量
CREATE TABLE IF NOT EXISTS tb_rs_total_msg_cnt
COMMENT "每日消息总量" AS
SELECT
    msg_day,
    COUNT(*) AS total_msg_cnt
FROM db_msg.tb_msg_etl
GROUP BY msg_day;


--指标2：统计每小时消息量、发送和接收用户数
CREATE TABLE IF NOT EXISTS tb_rs_hour_msg_cnt
COMMENT "每小时消息量趋势" AS
SELECT
    msg_hour,
    COUNT(*) AS total_msg_cnt,
    COUNT(DISTINCT sender_account) AS sender_usr_cnt,
    COUNT(DISTINCT receiver_account) AS receiver_usr_cnt
FROM db_msg.tb_msg_etl GROUP BY msg_hour;

--指标3：统计今日各地区发送消息总量
CREATE TABLE IF NOT EXISTS tb_rs_loc_cnt
COMMENT '今日各地区发送消息总量' AS
SELECT
    msg_day,
    sender_lng,
    sender_lat,
    COUNT(*) AS total_msg_cnt
FROM db_msg.tb_msg_etl
GROUP BY msg_day, sender_lng, sender_lat;

--指标4：统计今日发送和接收用户人数
CREATE TABLE IF NOT EXISTS tb_rs_usr_cnt
COMMENT "今日发送消息人数、接受消息人数" AS
SELECT
msg_day,
COUNT(DISTINCT sender_account) AS sender_usr_cnt,
COUNT(DISTINCT receiver_account) AS receiver_usr_cnt
FROM db_msg.tb_msg_etl
GROUP BY msg_day;

--指标5：统计发送消息条数最多的Top10用户
CREATE TABLE IF NOT EXISTS db_msg.tb_rs_r_user_top10
COMMENT "接收消息条数最多的Top10用户" AS
SELECT
receiver_name AS username,
COUNT(*) AS receiver_msg_cnt
FROM db_msg.tb_msg_etl
GROUP BY receiver_name
ORDER BY receiver_msg_cnt DESC
LIMIT 10;

--指标6：统计接收消息条数最多的Top10用户
CREATE TABLE IF NOT EXISTS db_msg.tb_rs_r_user_top10
COMMENT "接收消息条数最多的Top10用户" AS
SELECT
receiver_name AS username,
COUNT(*) AS receiver_msg_cnt
FROM db_msg.tb_msg_etl
GROUP BY receiver_name
ORDER BY receiver_msg_cnt DESC
LIMIT 10;

--指标7：统计发送人的手机型号分布情况

CREATE TABLE IF NOT EXISTS db_msg.tb_rs_sender_phone
COMMENT "发送人的手机型号分布" AS
SELECT
    sender_phonetype,
    COUNT(sender_account) AS cnt
FROM db_msg.tb_msg_etl
GROUP BY sender_phonetype;

--指标8：统计发送人的手机操作系统分布
--保存结果表
CREATE TABLE IF NOT EXISTS db_msg.tb_rs_sender_os
COMMENT "发送人的OS分布" AS
SELECT
    sender_os,
    COUNT(sender_account) AS cnt
FROM db_msg.tb_msg_etl
GROUP BY sender_os




