import pandas as pd 
import pickle
from nltk.corpus import sentiwordnet as swn
from nltk import pos_tag
from nltk import word_tokenize
import nltk
from nltk.tag.perceptron import PerceptronTagger
from nltk.corpus import brown

#zX--4nk6LshQ3D79HPACxw_Ch7NAhB_MWSDwcNbcptEKg
#USRXU4ASBAFSKhlk-KVrCw_Tm2dKN_-DCdZfF2xMWrX7w
#4sAbjATsbj5XfIDIAeHtXA_iwFoA98-OgcdXmNS0LxNOA
#dHgbL5EAEawIcqk6aXe2Ow_4tJiL2mHKO-erM6xoZji9Q
worst = "dHgbL5EAEawIcqk6aXe2Ow"
best = "4tJiL2mHKO-erM6xoZji9Q"

worst_pos = pickle.load(open('remove_stopwords/pickles/pos_scores_'+worst+'.pickle','rb'))
worst_neg = pickle.load(open('remove_stopwords/pickles/neg_scores_'+worst+'.pickle','rb'))

best_pos = pickle.load(open('remove_stopwords/pickles/pos_scores_'+best+'.pickle','rb'))
best_neg = pickle.load(open('remove_stopwords/pickles/neg_scores_'+best+'.pickle','rb'))


worst_df = pd.DataFrame(worst_pos.items(), columns=['A', 'B'])
best_df = pd.DataFrame(best_pos.items(), columns=['A', 'B'])


filter = pd.read_csv('remove_stopwords/words_list.csv', sep=',',header=None)
filter.columns =['A']


worst_df = worst_df[worst_df['A'].str.isalpha()]
best_df = best_df[best_df['A'].str.isalpha()]

combined_df = best_df[best_df['A'].isin(worst_df['A'])]
combined_df['C'] = worst_df['B']

objs = set()
pretrain = PerceptronTagger()

for row in combined_df.index:
	combined_df.at[row, 'B'] = (best_neg[combined_df.at[row, 'A']] - best_pos[combined_df.at[row, 'A']])
	combined_df.at[row, 'C'] = (worst_neg[combined_df.at[row, 'A']] - worst_pos[combined_df.at[row, 'A']])
	#print pretrain.tag([combined_df.at[row, 'A']])
	pos = pretrain.tag([combined_df.at[row, 'A']])[0][1]
	if pos == 'NN' or pos == 'NNP' or pos == 'NNS' or pos == 'NNPS':
		#print combined_df.at[row, 'A']+" " +pos
		objs.add(combined_df.at[row, 'A'])

temp = combined_df[combined_df['A'].isin(objs)]
temp = temp[temp['B']<0]
temp = temp[temp['C']>0]
temp = temp[~temp['A'].isin(filter['A'])]
temp = temp.sort_values('C',ascending=False)


temp.to_csv('combined/'+worst+'_'+best+'.csv', encoding='utf-8', index=False)


# worst_df=pd.read_csv('PageRank/'+worst+'_pageRankFile.txt', sep=',',header=None)
# best_df=pd.read_csv('PageRank/'+best+'_pageRankFile.txt', sep=',',header=None)


# w_max = worst_df["B"].max()
# w_min = worst_df["B"].min()
# w_diff = w_max -  w_min
# worst_df['B'] = worst_df['B']/w_diff

# for row in worst_df.index:
# 	if worst_df.at[row, 'A'] in worst_neg.keys() :
# 		worst_df.at[row, 'B'] = worst_df.at[row, 'B']*(worst_neg[worst_df.at[row, 'A']] - worst_pos[worst_df.at[row, 'A']])
# 	else :
# 		worst_df.at[row, 'B'] = 0

# worst_df = worst_df.sort_values('B',ascending=False)


# pd.concat([best_df, worst_df], axis=1, join='inner')


# worst_df.columns = ['A', 'B']
# best_df.columns = ['A', 'B']
