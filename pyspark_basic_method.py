#coding=utf-8
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType,ArrayType
from pyspark.sql.functions import udf as udf

#spark-shell多行粘贴进去
%cpaste
--

#Demo1:-------------------------------------------------------------------------------
#一些基础操作
#dataframe读取
df = spark.read.json("/xxxxx")
df = spark.read.text("/xxxxx")
df = spark.read.parquet("/xxxxx")
df = spark.read.orc("/xxxxx")

hdfs_path = '/xxxxx/xxxxx/xxxxx'
text = sc.textFile(hdfs_path)
def parser_func(data_json):
    j = json.loads(data_json)
    ret = []
    try:
        req = j.get('req')
        rsp = j.get('rsp')
        for res in rsp.get('adResults'):
            rsp_ad_group_id = res.get('adGroupId')
            rsp_creative_scores = res.get('creativeScores')
            for score in rsp_creative_scores:
                ret.append(
                    'req_id': req.get('req_id'),
                    'req_uid': req.get('user_id'),
                    'rsp_ad_group_id': rsp_ad_group_id,
                    'rsp_creative_id': score['creative_id'],
                    'rsp_pctr': score['pctr']
                )
            return ret
    except:
        pass
    return ret
df = spark.createDataFrame(text.flatMap(parser_func))

#排序
df.orderBy('exposure_cnt', ascending=False).show(50,False)

#返回前100的dataframe
df.limit(100)

#过滤
df.filter("name='hehe'") #保留等于hehe的行
df.filter("name like '%hehe%'") # *_hehe_*
df.filter(df.page_name.isin('综艺','动漫','纪录片','教育','会员'))
df.filter(df.name.isNotNull())) #保留name非空的，对应的空是：df.name.isNull()
df.where(df.name.contains('191'))

#类型转换
df = df.withColumn("item_id", df["item_id"].cast(LongType()))
df = df.withColumn('creative_type', F.col('creativeType').cast('string'))

#重命名
df = df.withColumnRenamed('item_id', 'new_item_id')

df.repartition(10).write.json(path=output_path, compression='snappy', mode='overwrite')

#Demo1: --------------------------------------------------------------------------
# df.groupBy后，使用agg函数
data = [
    {"aid":11, "type":"t1", "pctr": 0.2, "time": "2022-11-06 12:02"},
    {"aid":22, "type":"t2", "pctr": 0.25, "time": "2022-11-06 12:02"},
    {"aid":33, "type":"t1", "pctr": 0.6, "time": "2022-11-06 10:02"},
    {"aid":66, "type":"t2", "pctr": 0.6, "time": "2022-11-06 11:02"}
]
df = spark.createDataFrame(data)
df.printSchema()

#求平均
stat = df.groupBy("type").agg(F.mean("pctr").alias('mean_pctr'))
stat = df.groupBy('creative_id').agg(F.mean('pcvr').alias('avg_pcvr'), F.count('*').alias('count'))

#求和
sum_df = df.agg(F.sum("col_name").alias("sum_col"))
sum_col = sum_df.collect()[0]["sum_col"]

# Demo2: 使用groupby和F函数，统计各种比例。 统计pcvr小于0.005/0.01/0.05/0.1的占比，切区分流量
df = df.withColumn('is_huoshan', F.col('ppsABTag').like('%Huoshan%'))
stat = df.groupBy('is_huoshan', 'effect_type')\
         .agg(
            F.round(F.sum(F.when(F.col('pcvr')<0.005,1))/F.count('*'), 4).alias('pcvr_0.0005_ratio'),
            F.round(F.sum(F.when(F.col('pcvr')<0.01,1))/F.count('*'), 4).alias('pcvr_0.01_ratio'),
            F.round(F.sum(F.when(F.col('pcvr')<0.05,1))/F.count('*'), 4).alias('pcvr_0.05_ratio'),
            F.round(F.sum(F.when(F.col('pcvr')<0.1,1))/F.count('*'), 4).alias('pcvr_0.1_ratio'),
            F.count('*').alias('total_cnt')
         )

df= df.withColumn('is_huoshan', F.col('ppsABTag').like('%Huoshan%'))\
      .withColumn('flow_source',F.when('is_huoshan', 'Huoshan').otherwise('Huawei'))

# Demo2: ----------------------------------------
# F.concat_ws() 使用concat_ws将col1和col2列连接起来，中间用逗号分隔
df.withColumn("new_col", F.concat_ws(",", df.col1, df.col2))

#Demo2:---------------------------------------------------------------------------
# F.split切割字符串,
# |分割，则用\|切割
# $##$分割，则用\\$##\\$切割
data = [
    {"aid":11, "name":"aa|11"},
    {"aid":22, "name":"aa|22"},
    {"aid":33, "name":"bb|33"},
    {"aid":66, "name":"cc|66"}
]
df = spark.createDataFrame(data)
split_col = F.split(df['name'], '\|')
df2 = df.withColumn('name_1', split_col.getItem(0))

#Demo3:----------------------------------------------------------------------------
# 使用F.expr将 json object 解开
data = [
    {"aid":11, "obj1": {"oid": '1111', "slot_id":2}},
    {"aid":66, "obj1": {"oid": '6666', "slot_id":9}},
    {"aid":33, "obj1": {"oid": '3333', "slot_id":6}},
    {"aid":99, "obj1": {"oid": '9999', "slot_id":8}},
    {"aid":88, "obj1": {"oid": '8888', "slot_id":1}},
]
df = spark.createDataFrame(data)
df2 = df.withColumn("obj_id", F.expr('obj1.oid').cast('string'))


#Demo4:---------------------------------------------------------------------------
# 使用F.from_json解析字符串形式的json,把str类型转化成struct类型
import pyspark.sql.functions as F
import json
data = [
    {"aid":11, "obj1": json.dumps({"oid": '1111', "slot_id":2})},
    {"aid":66, "obj1": json.dumps({"oid": '6666', "slot_id":9})},
    {"aid":33, "obj1": json.dumps({"oid": '3333', "slot_id":6})},
    {"aid":99, "obj1": json.dumps({"oid": '9999', "slot_id":8})},
    {"aid":88, "obj1": json.dumps({"oid": '8888', "slot_id":1})},
]
df = spark.createDataFrame(data)

json_schema = spark.read.json(df.rdd.map(lambda row: row.obj1)).schema
df2 = df.withColumn('obj2', F.from_json(F.col('obj1'), json_schema))


#Demo5: --------------------------------------------------------------------------
# 使用F.regexp_extract使用正则表达式,提取部分字符串
data = [
    {"aid":11, "type":"t1", "pctr": 0.2, "ppsABTag": "aaa#perfmThr_Huoshan02#aaaaa"},
    {"aid":22, "type":"t2", "pctr": 0.25, "ppsABTag": "bbb#perfmThr_Huoshan01#bbbb"},
    {"aid":33, "type":"t1", "pctr": 0.6, "ppsABTag": "cc#perfmThr_Huoshan02#ccc"},
    {"aid":66, "type":"t2", "pctr": 0.6, "ppsABTag": "dd#perfmThr_Huoshan01#ddd"}
]
df = spark.createDataFrame(data)
df2 = df.withColumn('ab_version', F.regexp_extract('ppsABTag', r'(perfmThr.*?)(?=#)', 1))

#Demo6:-------------------------------------------------------------------------
#时间戳转北京时间
from pyspark.sql import functions as F
data = [
    {"aid":11, "req_time": 1670577463},
    {"aid":22, "req_time": 1670573538},
    {"aid":33, "req_time": 1670559140},
    {"aid":66, "req_time": 1670556740}
]
df = spark.createDataFrame(data)
df2 = df.withColumn("bj_time", F.from_unixtime(F.col("req_time")))


#Demo7:-------------------------------------------------------------------------
#用F.when条件，求当前行与上一行求差
from pyspark.sql import functions as F
from pyspark.sql.window import Window

data = [
    {"aid":11, "req_time": 1670577463, "value": 86},
    {"aid":22, "req_time": 1670573538, "value": 12},
    {"aid":33, "req_time": 1670559140, "value": 98},
    {"aid":66, "req_time": 1670556740, "value": 32}
]
df = spark.createDataFrame(data)
my_window = Window.partitionBy().orderBy("req_time")
df = df.withColumn("prev_value", F.lag(df.value).over(my_window))
df = df.withColumn("diff", F.when(F.isnull(df.value - df.prev_value), 0).otherwise(df.value - df.prev_value))


#Demo8: -----------------------------------------------------------------------
#各种join
rdf = adf.join(bdf, "item_id", "inner")
rdf = adf.join(bdf, ["req_id", "item_id"], "inner") #两个key来join
rdf = adf.join(bdf, "item_id", "left_anti") #在adf中，但不在bdf中
rdf = adf.join(bdf, "item_id", "left_semi") #left_semi用于返回跟左表一样schema的结果，最终结果是inner的结果


#Demo9：-----------------------------------------------------------------------------
#df某列是string类型，但是可以解析成json，使用F.from_json从json string中解析字段
data = [
    {"aid":11, "extra_info": json.dumps({"oid": '1111', "slot_id":2})},
    {"aid":66, "extra_info": json.dumps({"oid": '6666', "slot_id":9})},
    {"aid":33, "extra_info": json.dumps({"oid": '3333', "slot_id":6})},
    {"aid":99, "extra_info": json.dumps({"oid": '9999', "slot_id":8})},
    {"aid":88, "extra_info": json.dumps({"oid": '8888', "slot_id":1})}
]
df = spark.createDataFrame(data)

json_schema = spark.read.json(df.rdd.map(lambda row: row.extra_info)).schema
df2 = df.withColumn('extra_info', F.from_json(F.col('extra_info'), json_schema))
df3 = df2.withColumn('extra_info_id', F.col('extra_info.oid'))

#Demo10:-------------------------------------------------------------------------
#使用F.substring截取子字符串
df2 = df.withColumn("second_name", F.substring(df.name, 6, 10))

#Demo11:----------------------------------------------------------------------
#使用F.lit填充默认值
df2 = df.withColumn("status", F.lit(0)) #填充

#Demo12:-----------------------------------------------------------------------
#使用F.udf和lambda函数比较两列中相等的值
data = [
    {"aid":11, "col_a": 888, "col_b": 887},
    {"aid":22, "col_a": 666, "col_b": 666},
    {"aid":33, "col_a": 999, "col_b": 999},
    {"aid":66, "col_a": 555, "col_b": 555}
]
df = spark.createDataFrame(data)
df2 = df.withColumn('com_col_a&col_b', F.udf(lambda x,y: 1 if x==y else 0)(F.col('col_a'), F.col('col_b')))

#过滤两列不相等的行
df.filter(F.col('col_a') != F.col('col_b'))

#Demo13:-----------------------------------------------------------------------
#使用rdd.mapPartitions(xxx_func)解析api数据
def parse_log_data(partitions):
    for row in partitions: # partiton
        if row.rsp.value is None or row.rsp.value.data is None or row.req.rec is None:
            continue
        if row.req.rec.extra is None or row.rsp.value.meta is None:
            continue
        if row.req.reqId is None:
            continue
        for item in row.rsp.value.data: # item
            yield {
                "req_id": row.req.reqId,
                "user_id": row.req.user.uid,
                "scene_id": row.req.rec.extra.scene_id,
                "scene": row.req.scene,
                "origin_item_id": item.idStr,
                "parent_item_id": row.req.rec.extra.parent_item_id,
                "item_id": item.id,
                "spm": row.req.rec.spm,
                "version": row.rsp.value.meta.version,
                "datetime": row.datetime
            }

adf = spark.read.json("xxxx").repartition(1000).rdd.mapPartitions(parse_log_data).toDF()

#Demo14:-----------------------------------------------------------------------
#使用F.explode拍平数据
data = [('James',     ['Java','Scala'],      {'hair':'black','eye':'brown'}),
        ('Michael',   ['Spark','Java',None], {'hair':'brown','eye':None}),
        ('Robert',    ['CSharp',''],         {'hair':'red','eye':''}),
        ('Washington', None,                 None),
        ('Jefferson',  ['1','2'],            {})
]
df = spark.createDataFrame(data=data, schema = ['name','knownLanguages','properties'])
df.printSchema()

import pyspark.sql.functions as F
df2 = df.withColumn("knownLanguages_2", F.explode(df.knownLanguages))  # 如果F.explode的是一个array，比如说有3个元素，那该行会复制3条
df3 = df.select(df.name, F.explode(df.properties))          #


#Demo15:---------------------------------------------------------------------
#某一列string转long，然后长度跟原来长度比较，如果是float类型的string，则强转会丢小数部分
data = [
    {"contend_id": "181965", "col_a": 888},
    {"contend_id": "108926", "col_a": 666},
    {"contend_id": "162987", "col_a": 999},
    {"contend_id": "45.109828", "col_a": 555}
]
df = spark.createDataFrame(data)

df = df.withColumn('cid_check', F.col('contend_id').cast('long').cast('string'))\
       .filter(F.length(F.col('cid_check')) == F.length(F.col('contend_id')))
df.show()


#Demo16:----------------------------------------------------------------
#判断null
F.col("aid").isNotNull())
beh_data = beh_data.withColumn(c, F.when(F.col(c) == "null", F.lit("hello_null").cast('string')).otherwise(F.col(c)))


#Demo16:-----------------------------------------------------------------
#批量判断
target_list = [3666, 3888, 4999, 5893]
df2 = df.where(F.col('task_id').isin(target_list))

#Demo17:------------------------------------------------------------
#批量读取多天
spark.read.json('/user/xxxx/bhv/2023{0330,0401,0402}')


#Demo18:---------------------------------------------------------
#按照多列排序
df.orderBy(F.desc('cnt1'), F.desc('cnt2'))


#Demo19:--------------------
#window窗口函数的使用方法
#1.是用Window.partitionBy定义分区，表示窗口,这个最后跟groupBy不一样，这个还是dataframe
#2.在继续使用窗口函数操作，比如F.xxx().over(w)
#xxx可以是:
#    F.rank(), 用于为窗口中的每一行生成一个排名值。具有相同值的行将具有相同的排名
#    F.count(),
#    F.row_number(), 用于为窗口中的每一行生成一个唯一的行号，不考虑重复
#其中dropna() 函数会删除包含任何缺失值（NaN）的行。可以使用 axis 参数来指定删除行还是列，默认为删除行
from pyspark.sql import Window
w = Window.partitionBy('site_id','slot_id').orderBy(F.desc('rough_sort_cnt'), F.desc('adgroup_id'))
rough_top100 = df.select('*', F.rank().over(w).alias('rank')).where('rank <= 100')

#定义了window，他是按照site_id/slot_id分组，然后组内按照callback_cnt、adgroup_id降序排序，然后把window用在dataframe上，用rank排序截取前面100个
w = Window.partitionBy('site_id','slot_id').orderBy(F.desc('callback_cnt'), F.desc('adgroup_id'))
callback_top100 = df.dropna().select('*', F.rank().over(w).alias('rank')).where('rank <= 100')  # callback_cnt 可能为 NULL，需要 dropna
#定义的window，不光可以接上F.rank,也可以用F.count，比如df.withColumn("Count", F.count("*").over(window)).show()，就是分区后每组的个数统计

w2 = Window.partitionBy('site_id','slot_id')
df.withColumn('site_id_count', F.count('*').over(w2))





#Demo20:---------------------------------------
#有三列分别是A、B、C，现在要对A进行groupBy操作，然后根据B的不同值，创造新的列，并用C列的值填充进去
from pyspark.sql.functions import first
# 假设你的数据集名为 df，列名分别为 A、B、C
result = df.groupBy('A').pivot('B').agg(first('C'))
