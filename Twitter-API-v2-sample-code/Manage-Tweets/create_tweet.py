from requests_oauthlib import OAuth1Session
import os
import datetime
import json

# In your terminal please set your environment variables by running the following lines of code.
# export 'CONSUMER_KEY'='Ima4phgp68S9IAF96ANuu1jtf'
# export 'CONSUMER_SECRET'='79DOdZcoIUDA4xu2kwCDCzoaUT9agCSVbgqTvVt8S8PaYnVpCn'
# export 'oauth_token'='QkVUS052T1NmeExJRlhIZWN6Z1o6MTpjaQ'
# export 'oauth_token_secret'='vw1j_TE2FK5DPxPGqtmIWjyUv3kX6pNwz7Tg8vsfKOy3e8OscU'

consumer_key = "Ima4phgp68S9IAF96ANuu1jtf"
consumer_secret = "79DOdZcoIUDA4xu2kwCDCzoaUT9agCSVbgqTvVt8S8PaYnVpCn"

# Be sure to add replace the text of the with the text you wish to Tweet. You can also add parameters to post polls, quote Tweets, Tweet with reply settings, and Tweet to Super Followers in addition to other features.
payload = {"text": f"Hello world!AI {datetime.datetime.now()}"}

# # Get request token
# request_token_url = "https://api.twitter.com/oauth/request_token?oauth_callback=oob&x_auth_access_type=write"
# oauth = OAuth1Session(consumer_key, client_secret=consumer_secret)

# try:
#     fetch_response = oauth.fetch_request_token(request_token_url)
#     print("成功")
# except ValueError:
#     print(
#         "There may have been an issue with the consumer_key or consumer_secret you entered."
#     )

# resource_owner_key = fetch_response.get("oauth_token")
# resource_owner_secret = fetch_response.get("oauth_token_secret")
# print("Got OAuth token: %s" % resource_owner_key,resource_owner_secret)

# # Get authorization
# base_authorization_url = "https://api.twitter.com/oauth/authorize"
# authorization_url = oauth.authorization_url(base_authorization_url)
# print("Please go here and authorize: %s" % authorization_url)
# verifier = input("Paste the PIN here: ")
# print(verifier,type(verifier))
# # Get the access token
# access_token_url = "https://api.twitter.com/oauth/access_token"
# oauth = OAuth1Session(
#     consumer_key,
#     client_secret=consumer_secret,
#     resource_owner_key=resource_owner_key,
#     resource_owner_secret=resource_owner_secret,
#     verifier=verifier,
# )
# oauth_tokens = oauth.fetch_access_token(access_token_url)

# access_token = oauth_tokens["oauth_token"]
# access_token_secret = oauth_tokens["oauth_token_secret"]
access_token = "EOgbeAAAAAABoaTQAAABiPaQhhk"
access_token_secret = "Rpb2GJKDsoprPKrxbgg3dOnFpJYiPZXJ"

# Make the request
oauth = OAuth1Session(
    consumer_key,
    client_secret=consumer_secret,
    resource_owner_key=access_token,
    resource_owner_secret=access_token_secret,
)

# Making the request
response = oauth.post(
    "https://api.twitter.com/2/tweets",
    json=payload,
)

if response.status_code != 201:
    raise Exception(
        "Request returned an error: {} {}".format(response.status_code, response.text)
    )

print("Response code: {}".format(response.status_code))

# Saving the response as JSON
json_response = response.json()
print(json.dumps(json_response, indent=4, sort_keys=True))