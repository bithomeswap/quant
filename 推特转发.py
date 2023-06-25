import tweepy

# 填入你的 Twitter API 密钥和访问令牌
consumer_key = 'GWQVo6k0KAv65bfCDgQ2vJdS8'
consumer_secret = '58TLdDBDjnS1vB4I5pLA3IjRFDVrhekKNjJBobeBxb45RLsba7'
access_token = '773393572724899840-Mf5DsHPodPfufGobxxKrJsdL4TkYnwc'
access_token_secret = 'RNHtcnkVNss2OmFTpdq6ARn16EyX8mZblHwIvTzMAkMdn'

# 认证 Twitter API 访问
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

# 获取今日头条内容
# TODO: 在此处编写获取今日头条内容的代码，并将其存储在变量中
# 发布到 Twitter
# TODO: 在此处编写将今日头条内容转发到 Twitter 的代码
api.update_status('转发今日头条内容')

print('转发成功！')