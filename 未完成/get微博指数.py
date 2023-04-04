import akshare as ak
weibo_index_df = ak.weibo_index(word="期货", time_type="3month")
print(weibo_index_df)
微博指数数据
接口: weibo_index

目标地址: https: // data.weibo.com/index/newindex

描述: 获取指定 word 的微博指数

输入参数

名称	类型	必选	描述
word	str	Y	word = "股票"
time_type	str	Y	time_type = "1hour"
1hour, 1day, 1month, 3month 选其一
输出参数

名称	类型	默认显示	描述
date	datetime	Y	日期-索引
index	float	Y	指数
接口示例

weibo_index_df = ak.weibo_index(word="期货", time_type="3month")
print(weibo_index_df)
数据示例

期货
index
20190901  13334
20190902  46214
20190903  49017
20190904  53229
20190905  68506
...
20191127  68081
20191128  42348
20191129  62141
20191130  23448
