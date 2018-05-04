import tweepy as tw
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from textblob import TextBlob as tb
import csv, re

class SentimentAnalysis():

    def __init__(self):
        self.tweets = []
        self.tweetText = []

    def TWApi(self):
        consumerKey = 'ZCvyabcAFd33QxACOPHZYpJ7g'
        consumerSecret = 'Xmg8djNWLnqu0uZ03X19oIeU739LRM08LXIyByaHyrYhQFaizr'
        accessToken = '143385824-3AmSnBhPbzVsrKjqJb0JXMDKQFg1OcwdpiLw3HUX'
        accessTokenSecret = '7iTh1Tu1H8nVsdhZ7CSOTtbIEmlQF5QKhqKFdLfiHZFZe'

        auth = tw.OAuthHandler(consumerKey, consumerSecret)
        auth.set_access_token(accessToken, accessTokenSecret)
        api = tw.API(auth)
        return api

    def DownloadData(self):
        api = self.TWApi()
        searchTerm = input("Get me any string to search on: ")
        NoOfTerms = int(input("How many tweets you wanna search for "+searchTerm+": "))

        tweets = api.user_timeline(screen_name=searchTerm, count=NoOfTerms)

        csvFile = open('result.csv', 'a')
        csvWriter = csv.writer(csvFile)

        polarity = 0
        positive = 0
        negative = 0
        neutral = 0
        count = 0

        for tweet in tweets:
            self.tweetText.append(self.cleanIT(tweet.text).encode('utf-8'))
            analysis = tb(tweet.text)
            count += 1
            print(tweet.text, analysis.subjectivity)
            polarity += analysis.sentiment.polarity  # adding up polarities to find the average later
            if (analysis.sentiment.polarity == 0):  # adding reaction of how people are reacting to find average later
                neutral += 1
            elif (analysis.sentiment.polarity > 0 and analysis.sentiment.polarity <= 1):
                positive += 1
                print("positive Tweet:", tweet.text)
            elif (analysis.sentiment.polarity > -1 and analysis.sentiment.polarity <= 0):
                negative += 1
                print("negative tweet:", tweet.text)


        NoSearch = count
        print()
        print("Tweets that got pulled",count)
        csvWriter.writerow(self.tweetText)
        csvFile.close()

        positive = self.percentage(positive, count)
        negative = self.percentage(negative, count)
        neutral = self.percentage(neutral, count)
        polarity = polarity / NoOfTerms
        print("How people are reacting on " + searchTerm + " by analyzing " + str(NoSearch) + " tweets.")
        print()
        print("General Report: ")

        if (polarity == 0):
            print("Neutral")
        elif (polarity > 0 and polarity <= 1):
            print("Positive")
        elif (polarity > -1 and polarity <= 0):
            print("Negative")

        print()
        print("Detailed Report: ")
        print(str(positive) + "% people thought it was positive")
        print(str(negative) + "% people thought it was negative")
        print(str(neutral) + "% people thought it was neutral")
        print()

        self.plotPieChart(positive, negative, neutral, searchTerm, NoSearch)

    def cleanIT(self, tweet):
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) | (\w +:\ / \ / \S +)", " ", tweet).split())

    def percentage(self, part, whole):
        temp = (100 * float(part)) / float(whole)
        return format(temp, '.2f')

    def plotPieChart(self, positive, negative, neutral, searchTerm, NoSearch):
        labels = ['Positive [' + str(positive) + '%]',  'Neutral [' + str(neutral) + '%]',
                  'Negative [' + str(negative) + '%]']
        sizes = [positive, neutral, negative]
        colors = ['darkgreen','gold','red']
        patches, texts = plt.pie(sizes, colors=colors, startangle = 90 )
        plt.legend(patches, labels, loc="best")
        plt.title('How people are reacting on ' + searchTerm + ' by analyzing ' + str(NoSearch) + ' Tweets.')
        plt.axis('equal')
        plt.tight_layout()
        plt.show()

    def Plots(self):
        api = self.TWApi()

        searchTerm = input("Get me String to search: ")
        NoTerms = int(input("How many tweets you wanna search"+searchTerm+": "))

        TWT = api.user_timeline(screen_name=searchTerm, count=NoTerms)
        data = pd.DataFrame(data=[tweet.text for tweet in TWT], columns=['Tweets'])

        print("3 recent tweets:\n")
        for tweet in TWT[:3]:
            print(tweet.text)
            print()
        data['len'] = np.array([len(tweet.text) for tweet in TWT])
        data['ID'] = np.array([tweet.id for tweet in TWT])
        data['Date'] = np.array([tweet.created_at for tweet in TWT])
        data['Source'] = np.array([tweet.source for tweet in TWT])
        data['Likes'] = np.array([tweet.favorite_count for tweet in TWT])
        data['RTs'] = np.array([tweet.retweet_count for tweet in TWT])
        # data['Place'] = np.array([tweet.place for tweet in TWT])
        # data['user'] = np.array([tweet.user for tweet in TWT])
        # data['Geo'] = np.array([tweet.geo for tweet in TWT])

        mean = np.mean(data['len'])
        print("The lenght's average in tweets: {}".format(mean))
        print()
        fav_max = np.max(data['Likes'])
        rt_max = np.max(data['RTs'])
        fav = data[data.Likes == fav_max].index[0]
        rt = data[data.RTs == rt_max].index[0]
        print()

        print("Tweets with max likes: \n{}".format(data['Tweets'][fav]))
        print("Number of likes: {}".format(fav_max))
        print("{} characters.\n".format(data['len'][fav]))

        print("Tweets with max retweets: \n{}".format(data['Tweets'][rt]))
        print("Number of retweets: {}".format(rt_max))
        print("{} characters.\n".format(data['len'][rt]))

        tlen = pd.Series(data=data['len'].values, index=data['Date'])
        tfav = pd.Series(data=data['Likes'].values, index=data['Date'])
        tret = pd.Series(data=data['RTs'].values, index=data['Date'])

        tlen.plot(figsize=(16, 4), color='r')

        plt.plot(tfav, 'bo')
        plt.plot(tret, 'go')
        plt.show()

if __name__== "__main__":

    sa = SentimentAnalysis()
    sa.DownloadData()
    sa.Plots()
