# system import
from tqdm import tqdm
import pandas as pd
import os
# third-party import
import psycopg2
import spacy
# local import
from airbnb_disorder_analytics.config.db_config import DBInfo
from airbnb_disorder_analytics.analytics.query_reviews import POSExtractor
from airbnb_disorder_analytics.analytics.query_reviews import QueryReview


def find_adp(sentence):
    adps = [token for token in sentence if token.pos_ == 'ADP']
    return adps

sentence = 'i see sketchy people wandering throughout the neighbor'
qr = QueryReview()
pose = POSExtractor(qr.nlp(sentence))
adps = find_adp(pose.sentence)
extracted = []
for adp in adps:  # todo add a flag of visited to each adp
    pp = [token for token in adp.subtree]
    has_neighborhood = False
    for word in pp:
        if word.lower_ in qr.list_of_nouns_for_neighborhood:
            has_neighborhood = True
    first_part = []
    second_part = pp
    if has_neighborhood:
        for token in adp.ancestors:
            print('token:', token)
            if token.pos_ in pose.pos_aux:
                svaos = []
                subs, verb_negated = pose.get_all_subs(token)
                print('subs:', subs)
                # hopefully there are subs, if not, don't examine this verb any longer
                if len(subs) > 0:
                    more_subs = []
                    for sub in subs:
                        more_subs += [sub]
                        for right in sub.rights:
                            if right.dep_ in pose.PREPOSITIONS:
                                more_subs += list(right.subtree)
                    print('more subs:', more_subs)
                    # get adj for subs
                break
            elif token.dep_ in pose.PREPOSITIONS:
                print('adp:', adp, list(token.subtree))
    print(first_part, list(adp.ancestors), second_part)


