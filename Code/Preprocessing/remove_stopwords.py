import nltk
import re
from glob import glob
nltk.download('stopwords')
nltk.download('punkt')
from nltk.corpus import stopwords
import pandas as pd
from nltk.tokenize import word_tokenize
import string

stop_words = set(stopwords.words('english'))
ps = nltk.PorterStemmer(nltk.PorterStemmer.MARTIN_EXTENSIONS)


def remove_stopwords(x) :
	for row in x.index:
		long_text = x.at[row, 'text'].lower()
		long_text = long_text.replace('\'','')
		long_text = re.sub('[^a-zA-Z0-9\.]', ' ', long_text)
		#print long_text
		word_tokens = word_tokenize(long_text)
		filtered_sentence = [w for w in word_tokens if w not in stop_words and ps.stem(w) not in stop_words] 
		short_text = ' '.join(map(str, filtered_sentence)) 
		x.at[row, 'text']= short_text
	return x


for original_filename in glob('*.csv'):
    csv_filename = '%s_processed.csv' % original_filename[:-4]
    df = pd.read_csv(original_filename)
    df_converted = remove_stopwords(df)
    df_converted = df_converted[['user_id', 'text']]
    df_converted.to_csv(csv_filename, encoding='utf-8', index=False)





