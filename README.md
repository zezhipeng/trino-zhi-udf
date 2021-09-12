## 说明
这是一个 Trino 的 UDF/UDAF 包

## 如何使用
```
mvn clean package
```
复制 target/trino-zhi-udf-1-SNAPSHOT.jar 到 trino/plugin 文件夹下

## API

`word_count`(string) -> array<row<word varchar, pos varchar, full_word varchar, count double>>

分词函数，返回一个数组，数据里面每一行为对应词的相关统计信息

- word 词
- pos 词性
- full_word 词+词性
- count 次数统计

`funnel`(timestamp_col unix_time, window_size double, event_col varchar, events varchar) -> long

参数说明
- timestamp_col 统计的时间列
- window_size 统计的滑动时间窗口宽度
- event_col 事件值列
- events 事件集合的字符串，用`,`分割，如 'event_name1,event_name2,event_name3'

通过统计的时间列，去统计指定的滑动窗口内指定维度的事件值到达哪个流程，即如果只有 event_name1 行数据，那么返回结果为 1，存在 event_name1 和 event_name2，且 event_name1_timestamp < event_name2_timestamp
那么结果为 2

第二种传参数方式为*可变参数*针对多种事件为都定义为流程1的情况

`funnel`(timestamp_col unix_time, window_size double, event_col varchar, array<varchar> ... array<varchar>) -> long

`funnel_merge`(funnel long) -> array<long>
 
聚合 funnel 函数的统计结果
举例：
如果 funnel 返回结果只有一条 `4`，那么即为 `[1, 1, 1, 1]`
如果 funnel 返回结果只有两条 `3` 和 `4`，那么即为 `[2, 2, 2, 1]`


## 如何使用

### 分词统计

假设现在有一张评论表`comment`，数据如下

```
select * from comment limit 10;
                  content                   | from_customer_id 
--------------------------------------------+------------------
 这篇文章写的真好。用科学数据分析。              | 214855           
 这篇文章写的真好。                            | 214855           
 诚信为本，互利共赢！                          | 100913           
 黑土豆！含花青素高！抗癌抗衰老！美容养颜！       | 115642           
 好                                         | 224459           
 好                                         | 224459           
 20吨                                       | 116231           
 鲜蒜，50O吨                                 | 113050           
 以质论价                                    | 224704           
 价格5块8到6块1                              | 224704           
(10 rows)

Query 20210911_151507_00014_cbvfv, FINISHED, 1 node
Splits: 18 total, 18 done (100.00%)
0.69 [839 rows, 0B] [1.22K rows/s, 0B/s]
```

可以利用分词函数 word_count 对评论表进行分词统计，底层实现是使用 [ansj](https://github.com/NLPchina/ansj_seg/tree/f6774d635f1d82c43614c117d8962938e35af32d) 的 [nlp](https://nlpchina.github.io/ansj_seg/) 分词
再利用词性过滤去统计相关词性的词分布，词性参考[词性标注规范](https://github.com/NLPchina/ansj_seg/wiki/%E8%AF%8D%E6%80%A7%E6%A0%87%E6%B3%A8%E8%A7%84%E8%8C%83)

```sql
with words as (
    select
        word_count(a.content) as "nlp"
    from
        comment a
)
select
    word,
    num,
    pos
from
    words
cross join unnest(nlp) as t(word, pos, full_word, num)
where
    pos = 'ns'
```

返回结果
```
     word     | num | pos 
--------------+-----+-----
 周口         | 3.0 | ns  
 杞县         | 3.0 | ns  
 烟台         | 1.0 | ns  
 聊城         | 1.0 | ns  
 胶州市洋河镇  | 5.0 | ns  
 胶州         | 1.0 | ns  
 购金乡       | 1.0 | ns  
 河南         | 4.0 | ns  
 潍坊         | 2.0 | ns  
 印度         | 3.0 | ns  
(10 rows)

Query 20210911_151239_00012_cbvfv, FINISHED, 1 node
Splits: 35 total, 35 done (100.00%)
0.86 [839 rows, 0B] [973 rows/s, 0B/s]

```


用户埋点数据源

```
trino:tpch> select * from views limit 10;
     gmt_create      | user_id |       view       
---------------------+---------+------------------
 2021-07-30 01:04:54 | 1060848 | pages/home/index 
 2021-07-30 01:05:03 | 1060848 | pages/cat/index  
 2021-07-30 01:05:35 | 934233  | pages/cat/index  
 2021-07-30 01:07:20 | 885585  | pages/home/index 
 2021-07-30 01:11:42 | 1087421 | pages/home/index 
 2021-07-30 01:17:05 | 1088185 | pages/home/index 
 2021-07-30 01:17:11 | 1088185 | pages/cat/index  
 2021-07-30 01:17:27 | 1069203 | pages/home/index 
 2021-07-30 01:17:43 | 1069203 | pages/cat/index  
 2021-07-30 01:19:38 | 872079  | pages/home/index 
(10 rows)

Query 20210911_153140_00017_cbvfv, FINISHED, 1 node
Splits: 18 total, 18 done (100.00%)
0.22 [4.8K rows, 0B] [22K rows/s, 0B/s]
```

这里先通过 funnel 函数，以用户为维度，统计每个用户的访问深度，然后再利用 funnel_merge 函数对统计结果进行聚合，
得到指定路径下的用户访问深度

```sql
with a as (
    select
        funnel_merge("funnel") as "funnelVal"
    from
        (
        select
            a.user_id as "user_id",
            funnel(
                    to_unixtime(cast(a.gmt_create as timestamp)),
                    864000000,
                    a.view,
                    array ['pages/home/index'],
                    array ['pages/cat/index'],
                    array ['pages/cart/ack_order/index'],
                    array ['pages/order/pay/index']
                ) as "funnel"
        from
            views a
        group by
            a.user_id
        )
),
 steps as (
     select
         *
     from
         (
             values
                 ('首页', 1),
                 ('品类列表', 2),
                 ('购物车', 3),
                 ('付款页面', 4)
         ) t(step, idx)
 )
select
    steps.step,
    a.funnelVal [steps.idx]
from
    a
CROSS JOIN steps as steps
```

返回结果
```
   step   | _col1 
----------+-------
 首页     |   564 
 品类列表 |   449 
 购物车   |   286 
 付款页面 |   142 
(4 rows)

Query 20210911_152837_00015_cbvfv, FINISHED, 1 node
Splits: 67 total, 67 done (100.00%)
4.62 [4.8K rows, 0B] [1.04K rows/s, 0B/s]
```