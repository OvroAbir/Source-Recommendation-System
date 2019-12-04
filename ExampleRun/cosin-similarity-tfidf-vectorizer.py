import nltk, string
from sklearn.feature_extraction.text import TfidfVectorizer

stemmer = nltk.stem.porter.PorterStemmer()
remove_punctuation_map = dict((ord(char), None) for char in string.punctuation)

def stem_tokens(tokens):
    return [stemmer.stem(item) for item in tokens]

#'''remove punctuation, lowercase, stem'''
def normalize(text):
    return stem_tokens(nltk.word_tokenize(text.lower().translate(remove_punctuation_map)))

vectorizer = TfidfVectorizer(tokenizer=normalize, stop_words='english')

def cosine_sim(text1, text2):
    tfidf = vectorizer.fit_transform([text1, text2])
    return ((tfidf * tfidf.T).A)[0,1]



textinputfile="input1.txt"
file1 = open(textinputfile,"r")
text = file1.read()
# text = str(text.encode('ascii', "ignore"))
file1.close()
textinputfile="input2.txt"
file1 = open(textinputfile,"r")
text2 = file1.read()
# text = str(text.encode('ascii', "ignore"))
file1.close()
print cosine_sim(text, text2)