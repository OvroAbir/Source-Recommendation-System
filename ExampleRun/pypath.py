import nltk

print(nltk.__version__)
print(nltk.data.path)
nltk.data.path.append("/s/chopin/a/grad/joyghosh/nltk_data")
print("adding")
print(nltk.data.path)