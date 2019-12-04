def cosine_distance_countvectorizer_method(s1, s2):
    
    # sentences to list
    allsentences = [s1 , s2]
    
    # packages
    from sklearn.feature_extraction.text import CountVectorizer
    from scipy.spatial import distance
    
    # text to vector
    vectorizer = CountVectorizer()
    all_sentences_to_vector = vectorizer.fit_transform(allsentences)
    text_to_vector_v1 = all_sentences_to_vector.toarray()[0].tolist()
    text_to_vector_v2 = all_sentences_to_vector.toarray()[1].tolist()
    
    # distance of similarity
    cosine = distance.cosine(text_to_vector_v1, text_to_vector_v2)
    print('Similarity of two sentences are equal to ',round((1-cosine)*100,2),'%')
    return cosine


textinputfile="input.txt"
file1 = open(textinputfile,"r")
text1 = file1.read()
file1.close()
textinputfile="input2.txt"
file1 = open(textinputfile,"r")
text2 = file1.read()
# text = str(text.encode('ascii', "ignore"))
file1.close()
ss1 = text1
ss2 = text2
cosine_distance_countvectorizer_method(ss1 , ss2)