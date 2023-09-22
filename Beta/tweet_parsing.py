from tweet_parser.tweet import Tweet
from tweet_parser.tweet_parser_errors import NotATweetError, UnexpectedFormatError
import fileinput
import json
import string
import re
import csv
import geopy.distance
import nltk
nltk.download('vader_lexicon')
nltk.download('universal_tagset')
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
from nltk.util import ngrams
from nltk.sentiment import SentimentIntensityAnalyzer
from google.cloud import storage

# Create bucket client and authenticate via service account access key
storage_client = storage.Client.from_service_account_json('')

BUCKET_NAME = '' # Do not change

sia = SentimentIntensityAnalyzer()


names = [
    '09Dodgers', '10Giants', '11Diamondbacks',
    '12Cardinals', '13Reds', '14Cubs', '15Pirates', '16Brewers', '17Braves', '18Mets', '19Phillies', '20Marlins', '21Nationals',
    '22Indians', '23Twins', '24Royals', '25Tigers', '26WhiteSox', '27Yankees', '28Rays', '29RedSox', '30Orioles', '31BlueJays'
]

abbrevs = [
    'LAD', 'SFG', 'AZ', 'STL', 'CIN', 'CHC', 'PIT', 'MIL',
    'ATL', 'NYM', 'PHI', 'FLA', 'WSN', 'CLE', 'MIN', 'KCR', 'DET', 'CHW', 'NYY', 'TBD', 'BOS', 'BAL', 'TOR'
]

city_lats = {
    'ANA': 33.800031, 'OAK': 37.751614, 'SEA': 47.591217, 'HOU': 29.757224, 'TEX': 32.751207,
    'COL': 39.755878, 'SDP': 32.707206, 'LAD': 34.07358, 'SFG': 37.778361, 'AZ': 33.445498,
    'STL': 38.62248, 'CIN': 39.097392, 'CHC': 41.948036, 'PIT': 40.446993, 'MIL': 43.02834,
    'ATL': 33.890961, 'NYM': 40.756743, 'PHI': 39.905763, 'FLA': 25.778194, 'WSN': 38.872705,
    'CLE': 41.496183, 'MIN': 44.981703, 'KCR': 39.051355, 'DET': 42.339308, 'CHW': 41.830087,
    'NYY': 40.829519, 'TBD': 27.768214, 'BOS': 42.346361, 'BAL': 39.283658, 'TOR': 43.641684
}

city_longs = {
    'ANA': -117.883017, 'OAK': -122.200574, 'SEA': -122.332721, 'HOU': -95.35521, 'TEX': -97.082635,
    'COL': -104.994192, 'SDP': -117.15706, 'LAD': -118.240147, 'SFG': -122.389712, 'AZ': -112.066694,
    'STL': -90.193205, 'CIN': -84.506852, 'CHC': -87.65569, 'PIT': -80.005987, 'MIL': -87.971451,
    'ATL': -84.467772, 'NYM': -73.845994, 'PHI': -75.166574, 'FLA': -80.219668, 'WSN': -77.007632,
    'CLE': -81.685699, 'MIN': -93.278072, 'KCR': -94.480666, 'DET': -83.048876, 'CHW': -87.63405,
    'NYY': -73.926739, 'TBD': -82.653295, 'BOS': -71.097631, 'BAL': -76.621801, 'TOR': -79.389235
}

super_list = [
    ['NOUN', 'VERB', 'ADV'], ['NOUN', 'VERB', 'ADP'], ['NOUN', 'VERB', 'ADV', 'ADJ'],
    ['NOUN', 'VERB', 'NOUN', 'ADP'], ['NOUN', 'VERB', 'ADJ'], ['NOUN', 'VERB', 'ADJ', 'NOUN']
]

def clean_tweet(tweet):
    # remove stock market tickers like $GE
    tweet = re.sub(r'\$\w*', '', tweet)

    # remove old style retweet text 'RT'
    tweet = re.sub(r'^RT[\s]+', '', tweet)

    # remove hyperlinks
    tweet = re.sub(r'https?:\/\/.*[\r\n]*', '', tweet)

    # remove hashtags
    tweet = re.sub(r'#', '', tweet)

    return tweet


# If dict is empty, initialize. Otherwise, increment
def increment_dict(key, dictionary):
    dictionary[key] = dictionary.get(key, 0) + 1


def write_dict_to_file(file_name, dictionary):
    with open(file_name, 'w') as new_file:
        for key, value in dictionary.items():
            new_file.write('%s : %d\n' % (key, value))

            
def main():

    # ALL TEAMS DICTS
    ALL_TEAMS_bow = {}
    ALL_TEAMS_total_tweets = {}
    
    
    for team_name in names:

        team_abbrev = abbrevs[names.index(team_name)]

        dates = []

        for i in range(16, 20):
            # Atlanta 2016 is in a different stadium. Skip.
            if team_abbrev == 'ATL' and i == 16:
                continue
            with open(('Data/Team_WL/' + str(team_abbrev) + str(i) + '.csv'), newline='', encoding='utf-8-sig') as csvfile:
                reader = csv.reader(csvfile, delimiter=',', quotechar='|')
                for row in reader:
                    date = row[0]
                    if date.startswith('d'):
                        continue
                    if not date.startswith('1'):
                        date = '0' + date
                    if not date[5] == '/':
                        date = date[:3] + '0' + date[3:]
                    date = date[6:] + '-' + date[:2] + '-' + date[3:-5]
                    dates.append(date)
                    
                    
        # Average distances between tweets and stadium
        COM_average_distance = 0
        IP_average_distance = 0
        NP_average_distance = 0

        # COMBINED DICTS

        COM_bow = {}
        COM_total_tweets = {}
        COM_tweets_per_hour = {}

        # DICTS FOR TEAM IS PLAYING

        IP_bow = {}

        # Dictionaries: Key - Date/Time    Value - Number of tweets
        IP_total_tweets = {}
        IP_tweets_per_hour = {}

        IP_neg_polarity = {}
        IP_neu_polarity = {}
        IP_pos_polarity = {}
        IP_com_polarity = {}

        IP_yes_cognition = {}
        IP_no_cognition = {}


        # DICTS FOR TEAM IS NOT PLAYING
        
        NP_bow = {}

        # Dictionaries: Key - Date/Time    Value - Number of tweets
        NP_total_tweets = {}
        NP_tweets_per_hour = {}

        NP_neg_polarity = {}
        NP_neu_polarity = {}
        NP_pos_polarity = {}
        NP_com_polarity = {}

        NP_yes_cognition = {}
        NP_no_cognition = {}

        
        object_generator = storage_client.list_blobs(BUCKET_NAME, prefix=team_name, delimiter=None)

        for status_log_bucket_name in object_generator:

            client = storage.Client()
            BUCKET_NAME_TWO = client.bucket(BUCKET_NAME)
            blob = BUCKET_NAME_TWO.blob(status_log_bucket_name.name)

            with blob.open('rt') as f:
                for line in f:
                    try:
                        raw_tweet = json.loads(line)

                        tweet = Tweet(raw_tweet)

                        text = clean_tweet(tweet.all_text)

                        # Extracts time, date, language, and coordinates
                        date = tweet.created_at_string[0:10]
                        hour = int(tweet.created_at_string[11:13])
                        lang = tweet.lang
                        coordinates = tweet.geo_coordinates

                        # Increment combined total number of tweets
                        increment_dict(date, COM_total_tweets)
                        increment_dict(date, ALL_TEAMS_total_tweets)
                        increment_dict(hour, COM_tweets_per_hour)

                        # Increment combined total BoW
                        words = text.split()
                        for word in words:
                            new_word = word.translate(str.maketrans('', '', string.punctuation))
                            increment_dict(new_word, COM_bow)
                            increment_dict(new_word, ALL_TEAMS_bow)
                            

                        # Only operates on tweets with valid coordinates
                        if coordinates is not None and coordinates['latitude'] is not None and coordinates['longitude'] is not None:
                            tweet_lat = coordinates.get('latitude')
                            tweet_long = coordinates.get('longitude')

                            tweet_coords = (tweet_lat, tweet_long)

                            team_lat = city_lats[team_abbrev]
                            team_long = city_longs[team_abbrev]

                            team_coords = (team_lat, team_long)

                            # Distance between the tweet and stadium (meters)
                            distance = geopy.distance.geodesic(tweet_coords, team_coords).km * 1000

                            # Updates combined average distance
                            if COM_average_distance != 0:
                                COM_average_distance = COM_average_distance * (COM_total_tweets[date] - 1) / COM_total_tweets[date] + distance / COM_total_tweets[date]
                            else:
                                COM_average_distance = distance


                                
                        # !! TWEET PARSING !!

                        # If the team is playing

                        if date in dates and hour >= 10 and lang == 'en':

                            # Increment total number of tweets
                            increment_dict(date, IP_total_tweets)
                            increment_dict(hour, IP_tweets_per_hour)

                            # Increment total BoW
                            for word in words:
                                new_word = word.translate(str.maketrans('', '', string.punctuation))
                                increment_dict(new_word, IP_bow)

                            # Updates average distance
                            if coordinates is not None and coordinates['latitude'] is not None and coordinates['longitude'] is not None:
                                if IP_average_distance != 0:
                                    IP_average_distance = IP_average_distance * (IP_total_tweets[date] - 1) / IP_total_tweets[date] + distance / IP_total_tweets[date]
                                else:
                                    IP_average_distance = distance


                            # !! POLARITY !!

                            scores = sia.polarity_scores(text)  # Create a dict of scores

                            # Loop the dict of scores and increment the dicts if necessary
                            for key in scores:
                                if key == 'neg':
                                    if scores[key] == 0.0:
                                        increment_dict(date, IP_neg_polarity)
                                if key == 'neu':
                                    if scores[key] == 0.0:
                                        increment_dict(date, IP_neu_polarity)
                                if key == 'pos':
                                    if scores[key] == 0.0:
                                        increment_dict(date, IP_pos_polarity)
                                if key == 'compound':
                                    if scores[key] == 0.0:
                                        increment_dict(date, IP_com_polarity)


                            # !! COGNITION !!

                            tokens = nltk.word_tokenize(text)  # Create a list of tokens
                            tokens_tagged = nltk.pos_tag(tokens, tagset='universal')

                            pos_tokens = ''

                            # Loop the dict of tokens
                            for token in tokens_tagged:
                                pos_tokens += token[1] + ' '

                            sentence = str(pos_tokens)  # 'Whoever is happy will make others happy too'
                            trigrams = ngrams(sentence.split(), 3)
                            quadgrams = ngrams(sentence.split(), 4)

                            trigram_list = []
                            for item in trigrams:
                                trigram_list.append(item[0])
                                trigram_list.append(item[1])
                                trigram_list.append(item[2])

                            quadgram_list = []
                            for item in quadgrams:
                                quadgram_list.append(item[0])
                                quadgram_list.append(item[1])
                                quadgram_list.append(item[2])
                                quadgram_list.append(item[3])

                            # If in list, add to dict
                            if trigram_list in super_list or quadgram_list in super_list:
                                increment_dict(date, IP_yes_cognition)
                            else:
                                increment_dict(date, IP_no_cognition)


                        # If the team is NOT playing
                        
                        else:

                            # Increment total number of tweets
                            increment_dict(date, NP_total_tweets)
                            increment_dict(hour, NP_tweets_per_hour)

                            # Increment total BoW
                            for word in words:
                                new_word = word.translate(str.maketrans('', '', string.punctuation))
                                increment_dict(new_word, NP_bow)

                            # Updates average distance
                            if coordinates is not None and coordinates['latitude'] is not None and coordinates['longitude'] is not None:
                                if NP_average_distance != 0:
                                    NP_average_distance = NP_average_distance * (NP_total_tweets[date] - 1) / NP_total_tweets[date] + distance / NP_total_tweets[date]
                                else:
                                    NP_average_distance = distance


                            # !! POLARITY !!

                            scores = sia.polarity_scores(text)  # Create a dict of scores

                            # Loop the dict of scores and increment the dicts if necessary
                            for key in scores:
                                if key == 'neg':
                                    if scores[key] == 0.0:
                                        increment_dict(date, NP_neg_polarity)
                                if key == 'neu':
                                    if scores[key] == 0.0:
                                        increment_dict(date, NP_neu_polarity)
                                if key == 'pos':
                                    if scores[key] == 0.0:
                                        increment_dict(date, NP_pos_polarity)
                                if key == 'compound':
                                    if scores[key] == 0.0:
                                        increment_dict(date, NP_com_polarity)


                            # !! COGNITION !!

                            tokens = nltk.word_tokenize(text)  # Create a list of tokens
                            tokens_tagged = nltk.pos_tag(tokens, tagset='universal')

                            pos_tokens = ''

                            # Loop the dict of tokens
                            for token in tokens_tagged:
                                pos_tokens += token[1] + ' '

                            sentence = str(pos_tokens)  # 'Whoever is happy will make others happy too'
                            trigrams = ngrams(sentence.split(), 3)
                            quadgrams = ngrams(sentence.split(), 4)

                            trigram_list = []
                            for item in trigrams:
                                trigram_list.append(item[0])
                                trigram_list.append(item[1])
                                trigram_list.append(item[2])

                            quadgram_list = []
                            for item in quadgrams:
                                quadgram_list.append(item[0])
                                quadgram_list.append(item[1])
                                quadgram_list.append(item[2])
                                quadgram_list.append(item[3])

                            # If in list, add to dict
                            if trigram_list in super_list or quadgram_list in super_list:
                                increment_dict(date, NP_yes_cognition)
                            else:
                                increment_dict(date, NP_no_cognition)

                    except Exception as e:
                        # print(str(e))
                        pass


        # Write to combined average distance + total BoW/tweet text files
        with open(team_abbrev + '_COM_average_distance.txt', 'w') as new_file:
            new_file.write(str(COM_average_distance))
        write_dict_to_file(team_abbrev + '_COM_bow.txt', COM_bow)
        write_dict_to_file(team_abbrev + '_COM_total_tweets.txt', COM_total_tweets)
        write_dict_to_file(team_abbrev + '_COM_tweets_per_hour.txt', COM_tweets_per_hour)


        # TEAM IS PLAYING

        # Write to average distance + total BoW/tweet text files
        with open(team_abbrev + '_IP_average_distance.txt', 'w') as new_file:
            new_file.write(str(IP_average_distance))
        write_dict_to_file(team_abbrev + '_IP_bow.txt', IP_bow)
        write_dict_to_file(team_abbrev + '_IP_total_tweets.txt', IP_total_tweets)
        write_dict_to_file(team_abbrev + '_IP_tweets_per_hour.txt', IP_tweets_per_hour)

        # Write to polarity text files
        write_dict_to_file(team_abbrev + '_IP_pol_negative.txt', IP_neg_polarity)
        write_dict_to_file(team_abbrev + '_IP_pol_neutral.txt', IP_neu_polarity)
        write_dict_to_file(team_abbrev + '_IP_pol_positive.txt', IP_pos_polarity)
        write_dict_to_file(team_abbrev + '_IP_pol_compound.txt', IP_com_polarity)

        # Write to cognition text files
        write_dict_to_file(team_abbrev + '_IP_cogn_yes.txt', IP_yes_cognition)
        write_dict_to_file(team_abbrev + '_IP_cogn_no.txt', IP_no_cognition)

        
        # TEAM NOT PLAYING:

        # Write to average distance text file
        with open(team_abbrev + '_NP_average_distance.txt', 'w') as new_file:
            new_file.write(str(NP_average_distance))

        # Write to total BoW/tweet text files
        write_dict_to_file(team_abbrev + '_NP_bow.txt', NP_bow)
        write_dict_to_file(team_abbrev + '_NP_total_tweets.txt', NP_total_tweets)
        write_dict_to_file(team_abbrev + '_NP_tweets_per_hour.txt', NP_tweets_per_hour)

        # Write to polarity text files
        write_dict_to_file(team_abbrev + '_NP_pol_negative.txt', NP_neg_polarity)
        write_dict_to_file(team_abbrev + '_NP_pol_neutral.txt', NP_neu_polarity)
        write_dict_to_file(team_abbrev + '_NP_pol_positive.txt', NP_pos_polarity)
        write_dict_to_file(team_abbrev + '_NP_pol_compound.txt', NP_com_polarity)

        # Write to cognition text files
        write_dict_to_file(team_abbrev + '_NP_cogn_yes.txt', NP_yes_cognition)
        write_dict_to_file(team_abbrev + '_NP_cogn_no.txt', NP_no_cognition)


    # ALL TEAMS

    # Write to total BoW/tweet text files
    write_dict_to_file('ALL_TEAMS_bow.txt', ALL_TEAMS_bow)
    write_dict_to_file('ALL_TEAMS_total_tweets.txt', ALL_TEAMS_total_tweets)

    print('El Fin')

if __name__ == '__main__':
    main()
    
