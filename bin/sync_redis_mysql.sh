#!/usr/bin/bash

# source ~/.bash_profile
source /etc/profile

log(){
  cur_date=`date +"%Y-%m-%d %H:%M:%S"`
  echo "$cur_date >> $*"
}

log "start...................."
alias resic-cli=/usr/local/redis-3.2.0/src/redis-cli
redis_dict_key="wkd:ch:dict"
redis_stat_key="wkd:ch:stat"
redis_cmd="redis-cli -h 172.31.0.169 -p 6379"
mysql_cmd="mysql -hsinaif-olap-1d3c3d52-vpc.cn-shenzhen-a.ads.aliyuncs.com -P10001 -uLTAIIHUosh8I6qja -phQ49WJoqlduwUkjqCjjjRGT4PmWty5 sinaif_olap"
# calc_day=`date +%Y%m%d`
calc_day="20190818"
redis_ch_dict="/tmp/redis_ch_dict.data"
redis_ch_stat="/tmp/redis_ch_stat.data"
table="mid_2005_apichannel_effect_realtime"

# get channel
log "1. $redis_cmd smembers ${redis_dict_key}:${calc_day}"
$redis_cmd smembers ${redis_dict_key}:${calc_day} > $redis_ch_dict

log "2. generate redis command raw data: $redis_ch_dict"
for channel in `cat $redis_ch_dict`
do
  redis_key=${redis_stat_key}:${calc_day}:${channel}
  pv=`$redis_cmd hget $redis_key pv`
  uv=`$redis_cmd hget $redis_key uv`
  newuv=`$redis_cmd hget $redis_key newuv`
  # echo "$redis_key pv:$pv uv:$uv newuv:$newuv"
  if [ -z $pv ];then pv=0;fi
  if [ -z $uv ];then uv=0;fi
  if [ -z $newuv ];then newuv=0;fi
  echo "$redis_key pv:$pv uv:$uv newuv:$newuv"
  utime=`date "+%Y-%m-%d %H:%M:%S"`
  sql="insert into $table (dt, channel, pv, uv, new_uv, utime) values ('${calc_day}', '${channel}', ${pv}, ${uv}, ${newuv}, '$utime') on duplicate key update pv = ${pv}, uv = ${uv}, new_uv = ${newuv}, utime = '$utime'"
  # echo "$mysql_cmd -e \"$sql\""
  $mysql_cmd -e "$sql"

done

log "end...................."