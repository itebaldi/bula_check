import re

from nltk.stem import SnowballStemmer

from bula_check.constants import LANGUAGES
from bula_check.tools import curry


def lowercase_text(text: str) -> str:
    """
    Convert a string to lowercase.

    Parameters
    ----------
    text : str
        Input text.

    Returns
    -------
    str
        Lowercased text.
    """
    return text.lower()


def remove_text_punctuation(text: str) -> str:
    """
    Remove punctuation characters from a string.

    Parameters
    ----------
    text : str
        Input text.

    Returns
    -------
    str
        Text without punctuation.
    """
    return re.sub(r"[^\w\s]", "", text)


@curry
def remove_text_stopwords(text: str, stop_words: set[str]) -> str:
    """
    Remove stop words from a string.

    Parameters
    ----------
    text : str
        Input text.
    stop_words : Collection[str]
        Collection of stop words to remove.

    Returns
    -------
    str
        Text without stop words.
    """
    stopword_set = {word.lower() for word in stop_words}
    return " ".join(
        token for token in text.split() if token.lower() not in stopword_set
    )


@curry
def stem_text(
    text: str,
    language: LANGUAGES,
    ignore_stopwords: bool = False,
) -> str:
    """
    Apply Snowball stemming to a string.

    Parameters
    ----------
    text : str
        Input text.
    language : LANGUAGES
        Language used by the Snowball stemmer.
    ignore_stopwords : bool, default=False
        Whether stop words should be ignored by the stemmer.

    Returns
    -------
    str
        Stemmed text.

    Raises
    ------
    ValueError
        If ``language`` is not supported by NLTK SnowballStemmer.
    """
    if language not in SnowballStemmer.languages:
        raise ValueError(
            f"Unsupported language '{language}'. "
            f"Supported languages are: {sorted(SnowballStemmer.languages)}."
        )

    stemmer = SnowballStemmer(language, ignore_stopwords=ignore_stopwords)
    return " ".join(stemmer.stem(token) for token in text.split())
