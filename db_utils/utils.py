from nltk.stem import PorterStemmer


def get_stem_word(word: str):
    stemmer = PorterStemmer()
    return stemmer.stem(word, to_lowercase=False)

if __name__ == "__main__":
    print(get_stem_word("workflowitems"))
