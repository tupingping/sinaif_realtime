#!/usr/bin/bash

source /etc/profile

log(){
  cur_date=`date +"%Y-%m-%d %H:%M:%S"`
  echo "$cur_date >> $*"
}

log "start...................."

redis_key="wkd:uid_past"
redis_cmd="redis-cli -h 172.31.0.169 -p 6379"
hive_cmd="beeline -u jdbc:hive2://172.31.0.121:10000"
calc_day=`date +%Y%m%d`
raw_data="/tmp/weibo_uid_past.raw"
clean_data="/tmp/weibo_uid_past.clean"
redis_raw_data="/tmp/weibo_uid_past.redis.raw"
redis_pipe_data="/tmp/weibo_uid_past.redis.pipe"
hql="set mapred.job.name=weibo_uid_past;select uid from sinaif.t_user_weibosynchdata where dt < '$calc_day' group by uid;";

cat /dev/null > $redis_raw_data
cat /dev/null > $redis_pipe_data

# get uid from hive
log "1. $hive_cmd -e \"$hql\""
$hive_cmd -e "$hql" > $raw_data
sed -e "s/ //g;s/|//g;/uid/d;/+/d" $raw_data > $clean_data

log "2. generate redis command raw data: $redis_raw_data"
for line in `cat $clean_data`
do
  echo "sadd $redis_key $line" >> $redis_raw_data
  # $redis_cmd sadd $redis_key $line
done

log "3. transform raw data to redis protocol data: $redis_pipe_data"
while read CMD; do
  # each command begins with *{number arguments in command}\r\n
  XS=($CMD); printf "*${#XS[@]}\r\n" >> $redis_pipe_data
  # for each argument, we append ${length}\r\n{argument}\r\n
  for X in $CMD; do printf "\$${#X}\r\n$X\r\n" >> $redis_pipe_data; done
done < $redis_raw_data

log "4. use redis pipeline to batch insert"
time cat $redis_pipe_data | $redis_cmd --pipe


log "end...................."