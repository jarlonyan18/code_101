#--------------------------------------------
#demo1:
target_date=`date --date="3 day ago" "+%Y-%m-%d"`
for((i=1; i<=10 ; i++))
do
    echo $target_date
    bash run_xxx.sh $target_date
    wait
    sleep 30
done


#--------------------------------------------
#demo2: 日期循环
start_date=20151101
end_date=20151103
start_sec=`date -d "$start_date" "+%s"`
end_sec=`date -d "$end_date" "+%s"`
for((i=start_sec;i<=end_sec;i+=86400)); do
    day=$(date -d "@$i" "+%Y-%m-%d")
    echo $day
done

#------------------------------------------
#demo3: 小时循环
for i in `seq -w 1 24`
do
   echo $i
done



ll -t xxx/ | tac

find . -name xxxx.log
