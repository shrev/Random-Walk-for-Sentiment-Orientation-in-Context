import nltk
import pandas as pd
from nltk.corpus import sentiwordnet as swn
from nltk import pos_tag
from nltk import word_tokenize
import networkx as nx
import pickle
from glob import glob

words = set()
pos_words = {}
neg_words = {}
pos_cutOff = 0.6
neg_cutOff = 0.6
obj_cutOff = 0.8
count =0

def sentimentRanks(data,filename) :
	words = set()
	pos_words = {}
	neg_words = {}
	pos_cutOff = 0.6
	neg_cutOff = 0.6
	obj_cutOff = 0.8
	count =0
	for row in data.index:
		review = data.at[ row,'text'] 
		# count += 1
		# print count
		#print review
		sentences = review.split('.')
		#print sentences
		tags = [pos_tag(word_tokenize(sent)) for sent in sentences]
		#print tags
		for t in tags :
			for w in t:
				#print w[0]
				if w[0] not in words :
					words.add(w[0])
					#print(extract(w[1]))
					if len(swn.senti_synsets(w[0])) >0 :
						synset = swn.senti_synsets(w[0])[0]
						#print synset
						if synset.obj_score() > obj_cutOff :
							continue
						if synset.pos_score() > pos_cutOff :
							pos_words[w[0]] = synset.pos_score()
						if synset.neg_score() > neg_cutOff :
							neg_words[w[0]] = synset.neg_score()
	edges = {}
	for row in data.index:
		review = data.at[ row,'text'] 
		sentences = review.split('.')
		for sentence in sentences :
			sentence = sentence.split(' ')
			sentence = [x for x in sentence if x is not '']
			for i in xrange(0,len(sentence)) :
				for j in xrange(i+1,len(sentence)) :
					if sentence[i] not in edges.keys() :
						edges[sentence[i]] = {}
					if sentence[j] not in edges.keys() :
						edges[sentence[j]] = {}
					if sentence[j] not in edges[sentence[i]] :
						edges[sentence[i]][sentence[j]] = 0
					if sentence[i] not in edges[sentence[j]] :
						edges[sentence[j]][sentence[i]] = 0
					edges[sentence[i]][sentence[j]] += 1
					edges[sentence[j]][sentence[i]] += 1
	for key in edges :
		s = sum(edges[key].values())
		for k in edges[key] :
			edges[key][k] = float(edges[key][k])/s
	G = nx.DiGraph()
	for key in edges.keys():
		for v in edges[key].keys():
			G.add_edge(key, v, weight=edges[key][v])
	positive_rank =  nx.pagerank(G, alpha=0.85 , max_iter=200, personalization=pos_words, weight='weight')
	negative_rank =  nx.pagerank(G, alpha=0.85 , max_iter=200,personalization=neg_words, weight='weight')
	print positive_rank
	with open('pickles/pos_scores_'+filename+'.pickle', 'w') as handle:
		pickle.dump(positive_rank, handle, protocol=pickle.HIGHEST_PROTOCOL)
	with open('pickles/neg_scores_'+filename+'.pickle', 'w') as handle:
		pickle.dump(negative_rank, handle, protocol=pickle.HIGHEST_PROTOCOL)


	# count =0

	# for keys in positive_rank:
	# 	if positive_rank[key]==negative_rank[key] :
	# 		count +=1
	# 	else :
	# 		print key



for original_filename in glob('*_processed.csv'):
	data = pd.read_csv(original_filename)
	filename = original_filename[:-14]
	sentimentRanks(data,filename)


#m3DVIvPsuLuA9OMSgWNLcQ
#original_filename = "m3DVIvPsuLuA9OMSgWNLcQ_processed.csv"
#zX--4nk6LshQ3D79HPACxw_Ch7NAhB_MWSDwcNbcptEKg
#original_filename = "zX--4nk6LshQ3D79HPACxw_processed.csv"
# original_filename = "Ch7NAhB_MWSDwcNbcptEKg_processed.csv"
# data = pd.read_csv(original_filename)
# filename = original_filename[:-14]
# sentimentRanks(data,filename)
