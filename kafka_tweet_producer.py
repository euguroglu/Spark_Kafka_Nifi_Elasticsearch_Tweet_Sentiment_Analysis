import pykafka
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import twitter_config
from afinn import Afinn

#Twitter developer account configurations
consumer_key = twitter_config.consumer_key
consumer_secret = twitter_config.consumer_secret
access_token = twitter_config.access_token
access_secret = twitter_config.access_secret

#Twitter developer account auth
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)
afinn = Afinn()

def construct_message(jdt):
	sending_data = json.loads('{}')
	sending_data['text'] = jdt['text']
	sending_data['sentimental_score'] = afinn.score(jdt['text'])
	return json.dumps(sending_data)


#Twitter Stream Listener
class KafkaPushListener(StreamListener):
	def __init__(self):
		#localhost:9092 = Default Zookeeper Producer Host and Port Adresses
		self.client = pykafka.KafkaClient("localhost:9092")

		#Get Producer that has topic name is Twitter
		self.producer = self.client.topics[bytes("twitter", "ascii")].get_producer()

	def on_data(self, data):
		#Producer produces data for consumer
		#Data comes from Twitter
		try:

			sending_data = construct_message(json.loads(data))
			self.producer.produce(bytes(sending_data, "ascii"))
			return True
		except KeyError:
			return True

	def on_error(self, status):
		print(status)
		return True

#Start tweetter stream
twitter_stream = Stream(auth, KafkaPushListener())

#Produce data that filter given hashtag(Tweets)
twitter_stream.filter(languages=['en'], track=['#Turkey'])
