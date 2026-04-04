import re
import unicodedata

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


def remove_text_accents(text: str) -> str:
    """
    Remove accent marks from a string.

    Parameters
    ----------
    text : str
        Input text.

    Returns
    -------
    str
        Text without accent marks.
    """
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_text_whitespace(text: str) -> str:
    """
    Collapse repeated whitespace and trim leading/trailing spaces.

    Parameters
    ----------
    text : str
        Input text.

    Returns
    -------
    str
        Text with normalized whitespace.
    """
    return re.sub(r"\s+", " ", text).strip()


def replace_text_underscores_with_spaces(text: str) -> str:
    """
    Replace underscores with spaces.

    Parameters
    ----------
    text : str
        Input text.

    Returns
    -------
    str
        Text with underscores replaced by spaces.
    """
    return text.replace("_", " ")


def replace_spaces_with_text_underscores(text: str) -> str:

    return text.replace(" ", "_")


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


@curry
def generate_ngram_range(
    text: str,
    ngram_range: tuple[int, int],
) -> list[str]:
    """
    Generate n-grams for a range of sizes from a text.

    Parameters
    ----------
    text : str
        Input text.
    ngram_range : tuple[int, int]
        Inclusive range of n-gram sizes.

    Returns
    -------
    list[str]
        List of generated n-grams.

    Raises
    ------
    ValueError
        If ``ngram_range`` is invalid.
    """
    min_n, max_n = ngram_range

    if min_n < 1 or max_n < min_n:
        raise ValueError(
            "ngram_range must contain positive integers with min_n <= max_n."
        )

    ngrams: list[str] = []
    for n in range(min_n, max_n + 1):
        ngrams.extend(generate_ngrams(text, n))

    return ngrams


@curry
def generate_ngrams(text: str, n: int) -> list[str]:
    """
    Generate n-grams from a text.

    Parameters
    ----------
    text : str
        Input text.
    n : int
        N-gram size.

    Returns
    -------
    list[str]
        List of space-joined n-grams.

    Raises
    ------
    ValueError
        If ``n`` is less than 1.
    """
    if n < 1:
        raise ValueError("n must be greater than or equal to 1.")

    tokens = text.split()

    if len(tokens) < n:
        return []

    return [" ".join(tokens[i : i + n]) for i in range(len(tokens) - n + 1)]
