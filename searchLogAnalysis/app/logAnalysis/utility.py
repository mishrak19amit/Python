import configparser
import logging
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize

config = configparser.ConfigParser()
config.read('config/config.ini', encoding='utf-8')
size = config['BATCH_DATA_SIZE']['row_size']
logfile = config['DATA_SOURCE']['logfile']


def get_logger():
    logging.basicConfig(filename=logfile, format='%(asctime)s %(levelname)s %(message)s', filemode='w')
    logger = logging.getLogger()
    # Setting the threshold of logger to DEBUG
    logger.setLevel(logging.INFO)
    return logger


def get_batch_size():
    return size


def get_tokens(text):
    tokens = word_tokenize(text)
    return tokens


def stem_string(text):
    text_list = get_tokens(text)
    ps = PorterStemmer()
    stem_text = ""
    for word in text_list:
        stem_word = ps.stem(word)
        stem_text += stem_word + " "
    return stem_text


if __name__ == "__main__":
    string_stem = stem_string("I am running")
    print(string_stem)
