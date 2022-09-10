from nltk.stem import PorterStemmer
from sqlalchemy.engine import make_url


def get_stem_word(word: str):
    stemmer = PorterStemmer()
    return stemmer.stem(word, to_lowercase=False)


def get_standard_db_url_from_sqla(sqla_url: str):
    url = make_url(sqla_url)
    try:
        url_driver = url.drivername.split("+")[1]
    except KeyError:
        return sqla_url
    return sqla_url.replace(f"+{url_driver}", "")


if __name__ == "__main__":
    print(get_stem_word("workflowitems"))
