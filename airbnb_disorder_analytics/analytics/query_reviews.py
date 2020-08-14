"""
query_reviews.py

this program query airbnb reviews by keywords to find
descriptions related to neighborhood disorder/problem

includes test case for:
    - the Airbnb2ACS class

ruilin
08/11/2020
"""

# system import
from tqdm import tqdm
import pandas as pd
import os
# third-party import
import psycopg2
import spacy
# local import
from airbnb_disorder_analytics.config.db_config import DBInfo


class POSExtractor:
    def __init__(self, sent):
        self.sentence = sent
        self.SUBJECTS = ["nsubj", "nsubjpass", "csubj", "csubjpass", "agent", "expl"]
        self.OBJECTS = ["dobj", "dative", "attr", "oprd"]
        self.ADJECTIVES = ["acomp", "advcl", "advmod", "amod", "appos", "nn", "nmod", "ccomp", "complm",
                           "hmod", "infmod", "xcomp", "rcmod", "poss", " possessive"]
        self.COMPOUNDS = ["compound"]
        self.PREPOSITIONS = ["prep"]

    def get_nouns_from_conjunctions(self, nouns):
        moreNouns = []
        # rights is a generator
        for noun in nouns:
            rights = list(noun.rights)
            rightDeps = {token.lower_ for token in rights}
            if "and" in rightDeps:
                moreNouns.extend([token for token in rights if token.pos_ == "NOUN" and
                                  token not in moreNouns])
                if len(moreNouns) > 0:
                    moreNouns.extend([token for token in rights if token.pos_ == "NOUN" and
                                      token not in moreNouns])
        return moreNouns

    def get_noun_compound(self, token):
        nouns = [token]
        nouns.extend(self.get_nouns_from_conjunctions([token]))
        return nouns

    def get_adjs_from_conjunctions(self, adjs):
        moreAdjs = []
        for adj in adjs:
            # rights is a generator
            rights = list(adj.rights)
            rightDeps = {token.lower_ for token in rights}
            if "and" in rightDeps:
                moreAdjs.extend([token for token in rights if token.dep_ in self.ADJECTIVES
                                 or token.pos_ == "ADJ"])
                if len(moreAdjs) > 0:
                    moreAdjs.extend(self.get_adjs_from_conjunctions(moreAdjs))
        return moreAdjs

    def get_all_adjs_for_keyword(self):
        noun_compound = []
        visited_nouns = set()
        for token in self.sentence:
            if token.pos_ == 'NOUN' and token not in visited_nouns:
                new_compound = self.get_noun_compound(token)
                visited_nouns.update(new_compound)
                noun_compound.append(new_compound)
        adjs_for_keywords = []
        for nouns in noun_compound:
            for noun in nouns:
                if noun.text.lower() in qr.list_of_nouns_for_neighborhood:
                    for noun in nouns:
                        adjs = [left for left in noun.lefts if left.dep_ in self.ADJECTIVES]
                        if len(adjs):
                            adjs.extend(self.get_adjs_from_conjunctions(adjs))
                        adjs_for_keywords += ['!'+adj.text.lower() if self.is_negated(adj)
                                                    else adj.text.lower() for adj in adjs]
        return adjs_for_keywords

    def get_all_objs_with_adjectives(self, v):
        # rights is a generator
        rights = list(v.rights)
        objs = [token for token in rights if token.dep_ in self.OBJECTS]

        if len(objs) == 0:
            objs = [token for token in rights if token.dep_ in self.ADJECTIVES]

        objs.extend(self.get_objs_from_prepositions(rights))

        potentialNewVerb, potentialNewObjs = self.get_obj_from_x_comp(rights)
        if potentialNewVerb is not None and potentialNewObjs is not None and len(potentialNewObjs) > 0:
            objs.extend(potentialNewObjs)
            v = potentialNewVerb
        if len(objs) > 0:
            objs.extend(self.get_objs_from_conjunctions(objs))
        return v, objs

    def get_objs_from_prepositions(self, deps):
        objs = []
        for dep in deps:
            if dep.pos_ == "ADP" and dep.dep_ == "prep":
                objs.extend(
                    [token for token in dep.rights if token.dep_ in self.OBJECTS or
                     (token.pos_ == "PRON" and token.lower_ == "me")])
        return objs

    def get_objs_from_attrs(self, deps):
        for dep in deps:
            if dep.pos_ == "NOUN" and dep.dep_ == "attr":
                verbs = [token for token in dep.rights if token.pos_ == "VERB"]
                if len(verbs) > 0:
                    for v in verbs:
                        rights = list(v.rights)
                        objs = [token for token in rights if token.dep_ in self.OBJECTS]
                        objs.extend(self.get_objs_from_prepositions(rights))
                        if len(objs) > 0:
                            return v, objs
        return None, None

    def get_obj_from_x_comp(self, deps):
        for dep in deps:
            if dep.pos_ == "VERB" and dep.dep_ == "xcomp":
                v = dep
                rights = list(v.rights)
                objs = [token for token in rights if token.dep_ in self.OBJECTS]
                objs.extend(self.get_objs_from_prepositions(rights))
                if len(objs) > 0:
                    return v, objs
        return None, None

    def get_all_subs(self, v):
        verbNegated = self.is_negated(v)
        subs = [token for token in v.lefts if token.dep_ in self.SUBJECTS and token.pos_ != "DET"]
        if len(subs) > 0:
            subs.extend(self.get_subs_from_conjunctions(subs))
        else:
            foundSubs, verbNegated = self.find_subs(v)
            subs.extend(foundSubs)
        return subs, verbNegated

    def get_all_objs(self, v):
        # rights is a generator
        rights = list(v.rights)
        objs = [token for token in rights if token.dep_ in self.OBJECTS]
        objs.extend(self.get_objs_from_prepositions(rights))

        potentialNewVerb, potentialNewObjs = self.get_obj_from_x_comp(rights)
        if potentialNewVerb is not None and potentialNewObjs is not None and len(potentialNewObjs) > 0:
            objs.extend(potentialNewObjs)
            v = potentialNewVerb
        if len(objs) > 0:
            objs.extend(self.get_objs_from_conjunctions(objs))
        return v, objs

    def get_subs_from_conjunctions(self, subs):
        moreSubs = []
        for sub in subs:
            # rights is a generator
            rights = list(sub.rights)
            rightDeps = {token.lower_ for token in rights}
            if "and" in rightDeps:
                moreSubs.extend([token for token in rights if token.dep_ in self.SUBJECTS or token.pos_ == "NOUN"])
                if len(moreSubs) > 0:
                    moreSubs.extend(self.get_subs_from_conjunctions(moreSubs))
        return moreSubs

    def get_objs_from_conjunctions(self, objs):
        moreObjs = []
        for obj in objs:
            # rights is a generator
            rights = list(obj.rights)
            rightDeps = {token.lower_ for token in rights}
            if "and" in rightDeps:
                moreObjs.extend([token for token in rights if token.dep_ in self.OBJECTS
                                 or token.pos_ == "NOUN"])
                if len(moreObjs) > 0:
                    moreObjs.extend(self.get_objs_from_conjunctions(moreObjs))
        return moreObjs

    def get_verbs_from_conjunctions(self, verbs):
        moreVerbs = []
        for verb in verbs:
            rightDeps = {token.lower_ for token in verb.rights}
            if "and" in rightDeps:
                moreVerbs.extend([token for token in verb.rights if token.pos_ == "VERB"])
                if len(moreVerbs) > 0:
                    moreVerbs.extend(self.get_verbs_from_conjunctions(moreVerbs))
        return moreVerbs

    def find_subs(self, token):
        head = token.head
        while head.pos_ != "VERB" and head.pos_ != "NOUN" and head.head != head:
            head = head.head
        if head.pos_ == "VERB":
            subs = [token for token in head.lefts if token.dep_ == "SUB"]
            if len(subs) > 0:
                verbNegated = self.is_negated(head)
                subs.extend(self.get_subs_from_conjunctions(subs))
                return subs, verbNegated
            elif head.head != head:
                return self.find_subs(head)
        elif head.pos_ == "NOUN":
            return [head], self.is_negated(token)
        return [], False

    def generate_sub_compound(self, sub):
        sub_compunds = []
        for token in sub.lefts:
            if token.dep_ in self.COMPOUNDS:
                sub_compunds.extend(self.generate_sub_compound(token))
        sub_compunds.append(sub)
        for token in sub.rights:
            if token.dep_ in self.COMPOUNDS:
                sub_compunds.extend(self.generate_sub_compound(token))
        return sub_compunds

    def generate_left_right_adjectives(self, obj):
        obj_desc_tokens = []
        for token in obj.lefts:
            if token.dep_ in self.ADJECTIVES:
                obj_desc_tokens.extend(self.generate_left_right_adjectives(token))
        obj_desc_tokens.append(obj)

        for token in obj.rights:
            if token.dep_ in self.ADJECTIVES:
                obj_desc_tokens.extend(self.generate_left_right_adjectives(token))

        return obj_desc_tokens

    def get_adjectives(self, tokens):
        nouns_with_adjs = []
        for token in tokens:
            if token.pos_ == 'NOUN':
                adjs = [left for left in token.lefts if left.dep_ in self.ADJECTIVES]
                if len(adjs):
                    adjs.extend(self.get_adjs_from_conjunctions(adjs))
                nouns_with_adjs.append(adjs + [token])
        return nouns_with_adjs

    def is_negated(self, token):
        negations = {"no", "not", "n't", "never", "none"}
        for dep in list(token.lefts) + list(token.rights):
            if dep.lower_ in negations:
                return True
        return False

    def find_svos(self):
        svos = []
        verbs = [token for token in self.sentence if token.pos_ == "VERB" and token.dep_ != "aux"]
        for v in verbs:
            subs, verbNegated = self.get_all_subs(v)
            # hopefully there are subs, if not, don't examine this verb any longer
            if len(subs) > 0:
                v, objs = self.get_all_objs(v)
                for sub in subs:
                    for obj in objs:
                        objNegated = self.is_negated(obj)
                        svos.append((sub.lower_, "!" + v.lower_ if verbNegated or objNegated else v.lower_, obj.lower_))
        return svos

    def find_svaos(self):
        svaos = []
        verbs = [token for token in self.sentence if token.pos_ == "AUX"]
        for v in verbs:
            subs, verbNegated = self.get_all_subs(v)
            # hopefully there are subs, if not, don't examine this verb any longer
            if len(subs) > 0:
                v, objs = self.get_all_objs_with_adjectives(v)
                for sub in subs:
                    for obj in objs:
                        objNegated = self.is_negated(obj)
                        obj_desc_tokens = self.generate_left_right_adjectives(obj)
                        sub_compound = self.generate_sub_compound(sub)
                        svaos.append((" ".join(token.lower_ for token in sub_compound),
                                     "!" + v.lower_ if verbNegated or objNegated else v.lower_,
                                     " ".join(token.lower_ for token in obj_desc_tokens)))
        return svaos

    def get_svaos_for_keyword(self):
        svaos = []
        verbs = [token for token in self.sentence if token.pos_ == "AUX"]
        for v in verbs:
            subs, verbNegated = self.get_all_subs(v)
            contains_keywords = False
            for sub in subs:
                if sub.text.lower() in qr.list_of_nouns_for_neighborhood:
                    contains_keywords = True
            # hopefully there are subs, if not, don't examine this verb any longer
            if len(subs) > 0 and contains_keywords:
                v, objs = self.get_all_objs_with_adjectives(v)
                for sub in subs:
                    for obj in objs:
                        objNegated = self.is_negated(obj)
                        obj_desc_tokens = self.generate_left_right_adjectives(obj)
                        svaos.append("!" +" ".join(token.lower_ for token in obj_desc_tokens)
                                        if verbNegated or objNegated else
                                     " ".join(token.lower_ for token in obj_desc_tokens))
        return svaos


class QueryReview:
    """
    get the ACS5 characteristics of the neighborhood to which each of the airbnb listings belongs
    """
    def __init__(self):
        # psql connection to the airbnb database
        self.airbnb_connection = psycopg2.connect(DBInfo.airbnb_config)
        self.airbnb_cursor = self.airbnb_connection.cursor()
        self.nlp_model = "en_core_web_sm"
        self.nlp = spacy.load(self.nlp_model)
        self.nlp.add_pipe(self.nlp.create_pipe('sentencizer'))  # updated
        self.root_path = '/home/rchen/Documents/github/airbnb_crime/airbnb_disorder_analytics'
        self.data_folder = 'analytics'
        self.batch_size = 1000
        self.type_of_nouns_filename = 'nouns_and_types.csv'
        self.list_of_nouns_for_neighborhood = None
        self._get_nouns_for_neighborhood()

    def _get_nouns_for_neighborhood(self):
        df = pd.read_csv(os.path.join(self.root_path, self.data_folder, 'labelled_nouns_and_types.csv'))
        self.list_of_nouns_for_neighborhood = df['noun'][df['type'] == 2].tolist()

    def count_occurences_of_keyword(self, key_words):
        query = """SELECT count(*)
                    FROM review
                    WHERE review_text LIKE ANY (values {})
                    ;
                    """.format(self.format_keywords_for_search(key_words))
        self.airbnb_cursor.execute(query)
        return self.airbnb_cursor.fetchone()[0]

    @staticmethod
    def format_keywords_for_search(key_words, full_word=True):
        if full_word:
            return ', '.join(['(\'% {} %\')'.format(key_word.replace("'", "''")) for key_word in key_words])
        else:
            return ', '.join(['(\'%{}%\')'.format(key_word.replace("'", "\'")) for key_word in key_words])

    def search_keywords(self, key_words, batch_size=None):
        if batch_size is None:
            batch_size = self.batch_size
        query = """SELECT reviewer_id, property_id, review_text
                    FROM review
                    WHERE review_text LIKE ANY (values {})
                    LIMIT {}
                    ;
                    """.format(self.format_keywords_for_search(key_words), batch_size)
        self.airbnb_cursor.execute(query)
        return self.airbnb_cursor.fetchall()

    def count_keywords(self, key_words):
        query = """SELECT count(*)
                    FROM review
                    WHERE review_text LIKE ANY (values {})
                    ;
                    """.format(self.format_keywords_for_search(key_words))
        self.airbnb_cursor.execute(query)
        return self.airbnb_cursor.fetchone()[0]

    def get_words_following(self, key_words):
        query = """SELECT review_text
                    FROM review
                    WHERE review_text LIKE ANY (values {})
                    LIMIT 5
                    ;
                    """.format(self.format_keywords_for_search(key_words))
        self.airbnb_cursor.execute(query)
        results = self.airbnb_cursor.fetchall()
        for result in results:
            for key_word in key_words:
                before_keyword, keyword, after_keyword = result[0].partition(key_word)
                print(keyword, '-BREAK-', after_keyword)

    def insert_nouns_into_psql(self, reviewer_id, property_id, list_of_words=None):
        if list_of_words is None:
            query = """UPDATE review
                        SET nouns_after_in_the = false
                        WHERE reviewer_id = %s
                        AND property_id = %s
                        ;
                        """
            self.airbnb_cursor.execute(query, (reviewer_id, property_id))
        else:
            query = """UPDATE review
                        SET nouns_after_in_the = %s
                        WHERE reviewer_id = %s
                        AND property_id = %s
                        ;
                        """
            self.airbnb_cursor.execute(query, (','.join(list_of_words), reviewer_id, property_id))
        self.airbnb_connection.commit()

    def get_noun_following_key_words(self, key_words=('in', 'on', 'around'), full_word=True):
        pos_set_for_noun = {'PROPN', 'NOUN'}
        # also: nearby
        query = """SELECT review_text, reviewer_id, property_id
                    FROM review
                    WHERE review_text LIKE ANY (values {})
                    AND nouns_after_in_the IS NULL
                    LIMIT {}
                    ;
                    """.format(self.format_keywords_for_search(key_words, full_word), self.batch_size)
        self.airbnb_cursor.execute(query)
        results = self.airbnb_cursor.fetchall()
        for result in tqdm(results, total=len(results)):
            doc = self.nlp(result[0])
            list_of_nouns_that_follow = []
            for word in doc:
                if word.text in key_words:
                    nouns_that_follow = [child.text.lower() for child in word.children if child.pos_ in pos_set_for_noun]
                    if len(nouns_that_follow):
                        list_of_nouns_that_follow += nouns_that_follow
            if len(list_of_nouns_that_follow):
                self.insert_nouns_into_psql(reviewer_id=result[1], property_id=result[2],
                                            list_of_words=list(set(list_of_nouns_that_follow)))
            else:
                self.insert_nouns_into_psql(reviewer_id=result[1], property_id=result[2])

    def get_noun_following_key_words_for_all_reviews(self, key_words=('in', 'on', 'around'), full_word=True):
        query = """SELECT count(*)
                    FROM review
                    WHERE review_text LIKE ANY (values {})
                    AND nouns_after_in_the IS NULL
                    ;
                    """.format(self.format_keywords_for_search(key_words, full_word))
        self.airbnb_cursor.execute(query)
        result = self.airbnb_cursor.fetchone()
        count_of_unlabelled_records = result[0]
        while count_of_unlabelled_records > 0:
            print('count of unlabelled records:', count_of_unlabelled_records)
            self.get_noun_following_key_words(key_words, full_word)
            count_of_unlabelled_records -= self.batch_size

    def get_all_nouns(self):
        query = """SELECT nouns_after_in_the
                    FROM review
                    WHERE nouns_after_in_the IS NOT NULL
                    AND nouns_after_in_the != 'false'
                    ;
                    """
        self.airbnb_cursor.execute(query)
        results = self.airbnb_cursor.fetchall()
        noun_set = set()
        for result in results:
            noun_set.update(result[0].split(','))
        print(f'found {len(noun_set)} different nouns')
        df = pd.DataFrame(columns=['noun', 'type'])
        df['noun'] = list(noun_set)
        df['type'] = 0
        df.to_csv(self.type_of_nouns_filename, index=False)
        print('saved to file', os.path.join(os.getcwd(), self.type_of_nouns_filename))

    def get_sentence_with_keywords(self, long_str):
        doc = self.nlp(long_str)
        sentences = [sent for sent in doc.sents for token in sent if token.text in self.list_of_nouns_for_neighborhood]
        return sentences

    def dependency_tree(self, sentence):
        print([(x.text,x.pos_,x.dep_,[(y.text,y.pos_) for y in list(x.children)]) for x in sentence
                if x.text in self.list_of_nouns_for_neighborhood])
        print([(x.text,x.pos_,x.dep_,[(y.text,y.pos_) for y in list(x.ancestors)]) for x in sentence
                if x.text in self.list_of_nouns_for_neighborhood])

    def get_unadj_records_with_keywords(self, batch_size=None):
        if batch_size is None:
            batch_size = self.batch_size
        query = """SELECT reviewer_id, property_id, review_text
                    FROM review
                    WHERE review_text LIKE ANY (values {values})
                    AND adjs_for_neighbor IS NULL
                    LIMIT {size}
                    ;
                    """.format(values=self.format_keywords_for_search(self.list_of_nouns_for_neighborhood),
                               size=batch_size)
        self.airbnb_cursor.execute(query)
        return self.airbnb_cursor.fetchall()

    def insert_adjs_into_psql(self, reviewer_id, property_id, list_of_words=None, verbose=False):
        if verbose:
            print(list_of_words)
        if list_of_words is None:
            query = """UPDATE review
                        SET adjs_for_neighbor = false
                        WHERE reviewer_id = %s
                        AND property_id = %s
                        ;
                        """
            self.airbnb_cursor.execute(query, (reviewer_id, property_id))
        else:
            query = """UPDATE review
                        SET adjs_for_neighbor = %s
                        WHERE reviewer_id = %s
                        AND property_id = %s
                        ;
                        """
            self.airbnb_cursor.execute(query, (','.join(list_of_words), reviewer_id, property_id))
        self.airbnb_connection.commit()

if __name__ == '__main__':
    #key_words = ['vandalize', 'vandalism', 'damage']
    #key_words = ['noise', 'noisy']  # or nois
    #key_words = ['graffiti']
    #key_words = ['on the street', 'in the neighbor', 'around the block', 'nearby', 'in the community', 'around the corner',
    #             'in the street', 'in the block']
    qr = QueryReview()
    qr.batch_size = 10000

    #print(f'found {qr.count_occurences_of_keyword(key_words)} occurences of {key_words}')
    #results = qr.search_keywords(key_words)

    #qr.get_noun_following_key_words_for_all_reviews(['in', 'on', 'around'])
    #qr.get_all_nouns()

    #get a sample of sentences with neighborhood-related keywords
    # print('count of reviews with neighborhood keywords:', qr.count_keywords(qr.list_of_nouns_for_neighborhood))
    # answer to this question is: 520044

    count_of_processed_records = 0
    while True:
        list_of_records = qr.get_unadj_records_with_keywords()
        if len(list_of_records) == 0:
            break
        for review_record in tqdm(list_of_records, total=qr.batch_size):
            reviewer_id, property_id, review_text = review_record
            sentences = qr.get_sentence_with_keywords(review_text)
            list_of_adjs = []
            for sentence in sentences:
                pose = POSExtractor(sentence)
                adjs = pose.get_all_adjs_for_keyword()
                list_of_adjs += adjs
                list_of_adjs += pose.get_svaos_for_keyword()
            qr.insert_adjs_into_psql(reviewer_id, property_id, list_of_words=list_of_adjs, verbose=False)
        count_of_processed_records += qr.batch_size
        print('total processed records:', count_of_processed_records)


# todo deal with i love this neighborhood