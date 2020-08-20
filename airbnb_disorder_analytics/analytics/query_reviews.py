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
        self.OBJECTS = ["dobj", "dative", "attr", "oprd", 'pobj']
        self.ADJECTIVES = ["acomp", "advcl", "advmod", "amod", "appos", "nn", "nmod", "ccomp", "complm",
                           "hmod", "infmod", "xcomp", "rcmod", "poss", " possessive"]
        self.COMPOUNDS = ["compound"]
        self.PREPOSITIONS = ["prep"]
        self.pos_aux = ['AUX', 'VERB']

    @staticmethod
    def get_nouns_from_conjunctions(nouns):
        more_nouns = []
        # rights is a generator
        for noun in nouns:
            rights = list(noun.rights)
            right_deps = {token.lower_ for token in rights}
            if "and" in right_deps:
                more_nouns.extend([token for token in rights if token.pos_ == "NOUN" and
                                  token not in more_nouns])
                if len(more_nouns) > 0:
                    more_nouns.extend([token for token in rights if token.pos_ == "NOUN" and
                                      token not in more_nouns])
        return more_nouns

    def get_noun_compound(self, token):
        nouns = [token]
        nouns.extend(self.get_nouns_from_conjunctions([token]))
        return nouns

    def get_adjs_from_conjunctions(self, old_list_of_adjs):
        more_adjs = []
        for adj in old_list_of_adjs:
            # rights is a generator
            rights = list(adj.rights)
            right_deps = {token.lower_ for token in rights}
            if "and" in right_deps:
                more_adjs.extend([token for token in rights if token.dep_ in self.ADJECTIVES
                                 or token.pos_ == "ADJ"])
                if len(more_adjs) > 0:
                    more_adjs.extend(self.get_adjs_from_conjunctions(more_adjs))
        return more_adjs

    def get_all_adjs(self, word):
        list_of_adjs = [right for right in word.rights if right.dep_ in self.ADJECTIVES]
        if len(list_of_adjs):
            list_of_adjs.extend(self.get_adjs_from_conjunctions(list_of_adjs))
        return list_of_adjs

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
            has_keyword = False
            for noun in nouns:
                if noun.text.lower() in qr.list_of_nouns_for_neighborhood:
                    has_keyword = True
            if has_keyword:
                for noun in nouns:
                    list_of_adjs = self.get_all_adjs(noun)
                    adjs_for_keywords += ['!'+adj.text.lower() if self.is_negated(adj)
                                          else adj.text.lower() for adj in list_of_adjs]
        return adjs_for_keywords

    def get_all_objs_with_adjectives(self, v):
        # rights is a generator
        rights = list(v.rights)
        objs = [token for token in rights if token.dep_ in self.OBJECTS or token.pos_ == 'NOUN']

        if len(objs) == 0:
            objs = [token for token in rights if token.dep_ in self.ADJECTIVES]

        objs.extend(self.get_objs_from_prepositions(rights))

        potential_new_verb, potential_new_objs = self.get_obj_from_x_comp(rights)
        if potential_new_verb is not None and potential_new_objs is not None and len(potential_new_objs) > 0:
            objs.extend(potential_new_objs)
            v = potential_new_verb
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
        verb_negated = self.is_negated(v)
        subs = [token for token in v.lefts if token.dep_ in self.SUBJECTS and token.pos_ != "DET"]
        if len(subs) > 0:
            subs.extend(self.get_subs_from_conjunctions(subs))
        else:
            found_subs, verb_negated = self.find_subs(v)
            subs.extend(found_subs)
        return subs, verb_negated

    def get_all_preps(self, v):
        verb_negated = self.is_negated(v)
        preps = [token for token in v.rights if token.dep_ in self.PREPOSITIONS]
        return preps, verb_negated

    def get_all_objs(self, v):
        # rights is a generator
        rights = list(v.rights)
        objs = [token for token in rights if token.dep_ in self.OBJECTS]
        objs.extend(self.get_objs_from_prepositions(rights))

        potential_new_verb, potential_new_objs = self.get_obj_from_x_comp(rights)
        if potential_new_verb is not None and potential_new_objs is not None and len(potential_new_objs) > 0:
            objs.extend(potential_new_objs)
            v = potential_new_verb
        if len(objs) > 0:
            objs.extend(self.get_objs_from_conjunctions(objs))
        return v, objs

    def get_subs_from_conjunctions(self, subs):
        more_subs = []
        for sub in subs:
            # rights is a generator
            rights = list(sub.rights)
            right_deps = {token.lower_ for token in rights}
            if "and" in right_deps:
                more_subs.extend([token for token in rights if token.dep_ in self.SUBJECTS or token.pos_ == "NOUN"])
                if len(more_subs) > 0:
                    more_subs.extend(self.get_subs_from_conjunctions(more_subs))
        return more_subs

    def get_objs_from_conjunctions(self, objs):
        more_objs = []
        for obj in objs:
            # rights is a generator
            rights = list(obj.rights)
            right_deps = {token.lower_ for token in rights}
            if "and" in right_deps:
                more_objs.extend([token for token in rights if token.dep_ in self.OBJECTS
                                 or token.pos_ == "NOUN"])
                if len(more_objs) > 0:
                    more_objs.extend(self.get_objs_from_conjunctions(more_objs))
        return more_objs

    def get_verbs_from_conjunctions(self, verbs):
        more_verbs = []
        for verb in verbs:
            right_deps = {token.lower_ for token in verb.rights}
            if "and" in right_deps:
                more_verbs.extend([token for token in verb.rights if token.pos_ == "VERB"])
                if len(more_verbs) > 0:
                    more_verbs.extend(self.get_verbs_from_conjunctions(more_verbs))
        return more_verbs

    def find_subs(self, token):
        head = token.head
        while head.pos_ != "VERB" and head.pos_ != "NOUN" and head.head != head:
            head = head.head
        if head.pos_ == "VERB":
            subs = [token for token in head.lefts if token.dep_ == "SUB"]
            if len(subs) > 0:
                verb_negated = self.is_negated(head)
                subs.extend(self.get_subs_from_conjunctions(subs))
                return subs, verb_negated
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
            if token.dep_ in self.ADJECTIVES or token.dep_ in self.SUBJECTS:
                obj_desc_tokens.extend(self.generate_left_right_adjectives(token))
        for token in obj.rights:
            if token.dep_ in self.ADJECTIVES or token.dep_ in self.SUBJECTS:
                obj_desc_tokens.extend(self.generate_left_right_adjectives(token))
            if token.pos_ == "ADP" and token.dep_ == "prep":
                obj_desc_tokens.extend(
                    [token for token in token.rights if token.dep_ in self.OBJECTS])
        return obj_desc_tokens +[obj]

    def get_adjectives(self, tokens):
        nouns_with_adjs = []
        for token in tokens:
            if token.pos_ == 'NOUN':
                tmp_list_of_adjs = [left for left in token.lefts if left.dep_ in self.ADJECTIVES]
                if len(tmp_list_of_adjs):
                    tmp_list_of_adjs.extend(self.get_adjs_from_conjunctions(tmp_list_of_adjs))
                nouns_with_adjs.append(tmp_list_of_adjs + [token])
        return nouns_with_adjs

    @staticmethod
    def is_negated(token):
        negations = {"no", "not", "n't", "never", "none"}
        for dep in list(token.lefts) + list(token.rights):
            if dep.lower_ in negations:
                return True
        return False

    def find_svos(self):
        svos = []
        verbs = [token for token in self.sentence if token.pos_ == "VERB" and token.dep_ != "aux"]
        if len(verbs) > 0:
            verbs.extend(self.get_verbs_from_conjunctions(verbs))
        for v in verbs:
            subs, verb_negated = self.get_all_subs(v)
            # hopefully there are subs, if not, don't examine this verb any longer
            if len(subs) > 0:
                v, objs = self.get_all_objs(v)
                for sub in subs:
                    for obj in objs:
                        this_obj = [obj]
                        obj_negated = self.is_negated(obj)
                        for dep in obj.children:
                            if dep.pos_ == "ADP" and dep.dep_ == "prep":
                                this_obj.extend(
                                [token for token in dep.rights if token.dep_ in self.OBJECTS or
                                 (token.pos_ == "PRON" and token.lower_ == "me")])
                        svos.append((sub.lower_, "!" + v.lower_ if verb_negated or obj_negated else
                                    v.lower_, ' '.join([word.lower_ for word in this_obj])))

        return svos

    def find_svaos(self):
        svaos = []
        verbs = [token for token in self.sentence if token.pos_ in self.pos_aux]
        for v in verbs:
            subs, verb_negated = self.get_all_subs(v)
            # hopefully there are subs, if not, don't examine this verb any longer
            if len(subs) > 0:
                v, objs = self.get_all_objs_with_adjectives(v)
                for sub in subs:
                    for obj in objs:
                        obj_negated = self.is_negated(obj)
                        obj_desc_tokens = self.generate_left_right_adjectives(obj)
                        obj_desc_tokens.extend(self.get_objs_from_conjunctions(obj_desc_tokens))
                        sub_compound = self.generate_sub_compound(sub)
                        svaos.append((" ".join(token.lower_ for token in sub_compound),
                                      "!" + v.lower_ if verb_negated or obj_negated else
                                      v.lower_, " ".join(token.lower_ for token in obj_desc_tokens)))
            preps, verb_negated = self.get_all_preps(v)
            if len(preps):
                for prep in preps:
                    _, objs = self.get_all_objs(prep)
                    for obj in objs:
                        obj_desc_tokens = self.generate_left_right_adjectives(obj)
                        svaos.append((prep.lower_, " ".join(token.lower_ for token in obj_desc_tokens)))
            adjs = self.get_all_adjs(v)
            for adj in adjs:
                preps, adj_negated = self.get_all_preps(adj)
                if len(preps):
                    for prep in preps:
                        _, objs = self.get_all_objs(prep)
                        for obj in objs:
                            obj_negated = self.is_negated(obj)
                            obj_desc_tokens = self.generate_left_right_adjectives(obj)
                            svaos.append(("!" + adj.lower_ if adj_negated or obj_negated else adj.lower_,
                            prep.lower_, " ".join(token.lower_ for token in obj_desc_tokens)))
        result = []
        for svao in svaos:
            for word in svao:
                if word not in result:
                    result.append(word)
        return result

    def get_svaos_for_keyword(self):
        svaos = []
        verbs = [token for token in self.sentence if token.pos_ in self.pos_aux]
        for v in verbs:
            subs, verb_negated = self.get_all_subs(v)
            contains_keywords = False
            for sub in subs:
                if sub.text.lower() in qr.list_of_nouns_for_neighborhood:
                    contains_keywords = True
            # hopefully there are subs, if not, don't examine this verb any longer
            if len(subs) > 0 and contains_keywords:
                v, objs = self.get_all_objs_with_adjectives(v)
                for obj in objs:
                    obj_negated = self.is_negated(obj)
                    obj_desc_tokens = self.generate_left_right_adjectives(obj)
                    svaos.append("!" + " ".join(token.lower_ for token in obj_desc_tokens)
                                 if verb_negated or obj_negated
                                 else " ".join(token.lower_ for token in obj_desc_tokens))
        return svaos

    def get_all_pronouns(self):
        pronouns = []
        for token in self.sentence:
            if token.pos_ == 'PRON':
                pronouns.append(token)
        return pronouns

    def get_all_verbs(self, p):
        verbs = [token for token in p.ancestors if token.pos_ == "VERB"]
        if len(verbs) > 0:
            verbs.extend(self.get_verbs_from_conjunctions(verbs))
        return verbs

    def get_pv_for_keyword(self):
        pvs = []
        pronouns = self.get_all_pronouns()
        for p in pronouns:
            verbs = self.get_all_verbs(p)
        return verbs


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
        df = pd.read_csv(os.path.join(self.root_path, self.data_folder, 'labelled_nouns_and_types_for_adjs.csv'))
        self.list_of_nouns_for_neighborhood = df['noun'][df['type'] == 2].tolist()
        print('nouns:', self.list_of_nouns_for_neighborhood)

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
                    nouns_that_follow = [child.text.lower() for child in word.children
                                         if child.pos_ in pos_set_for_noun]
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
        list_of_sentences = [sent for sent in doc.sents for token in sent
                             if token.text in self.list_of_nouns_for_neighborhood]
        return list_of_sentences

    def get_unadj_records_with_keywords(self, batch_size=None):
        if batch_size is None:
            batch_size = self.batch_size
        query = """SELECT reviewer_id, property_id, review_text
                    FROM review
                    WHERE review_text LIKE ANY (values {values})
                    AND really_short_adjs_for_neighbor IS NULL
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
                        SET really_short_adjs_for_neighbor = false
                        WHERE reviewer_id = %s
                        AND property_id = %s
                        ;
                        """
            self.airbnb_cursor.execute(query, (reviewer_id, property_id))
        else:
            query = """UPDATE review
                        SET really_short_adjs_for_neighbor = %s
                        WHERE reviewer_id = %s
                        AND property_id = %s
                        ;
                        """
            self.airbnb_cursor.execute(query, (','.join(list_of_words), reviewer_id, property_id))
        self.airbnb_connection.commit()




if __name__ == '__main__':
    # key_words = ['vandalize', 'vandalism', 'damage']
    # key_words = ['noise', 'noisy']  # or nois
    # key_words = ['graffiti']
    # key_words = ['on the street', 'in the neighbor', 'around the block', 'nearby',
    #               'in the community', 'around the corner',
    #             'in the street', 'in the block']
    qr = QueryReview()
    qr.batch_size = 10000

    # print(f'found {qr.count_occurences_of_keyword(key_words)} occurences of {key_words}')
    # results = qr.search_keywords(key_words)

    # qr.get_noun_following_key_words_for_all_reviews(['in', 'on', 'around'])
    # qr.get_all_nouns()

    # get a sample of sentences with neighborhood-related keywords
    # print('count of reviews with neighborhood keywords:', qr.count_keywords(qr.list_of_nouns_for_neighborhood))
    # answer to this question is: 520044


    # count_of_processed_records = 0
    # while True:
    #     list_of_records = qr.get_unadj_records_with_keywords()
    #     if len(list_of_records) == 0:
    #         break
    #     for review_record in tqdm(list_of_records, total=qr.batch_size):
    #         reviewer_id, property_id, review_text = review_record
    #         sentences = qr.get_sentence_with_keywords(review_text)
    #         list_of_adjs = []
    #         for sentence in sentences:
    #             pose = POSExtractor(sentence)
    #             adjs = pose.get_all_adjs_for_keyword()
    #             list_of_adjs += adjs
    #             list_of_adjs += pose.get_svaos_for_keyword()
    #         qr.insert_adjs_into_psql(reviewer_id, property_id, list_of_words=list_of_adjs, verbose=False)
    #     count_of_processed_records += qr.batch_size
    #     print('total processed records:', count_of_processed_records)


    # query = """SELECT ally_short_adjs_for_neighbor
    #             FROM review
    #             WHERE short_adjs_for_neighbor IS NOT NULL
    #             AND short_adjs_for_neighbor != 'false'
    #             AND short_adjs_for_neighbor != ''
    #             ;
    #             """
    # qr.airbnb_cursor.execute(query)
    # results = qr.airbnb_cursor.fetchall()
    # word_freq_dict = {}
    # for result in results:
    #     for word in result[0].split(','):
    #         word_freq_dict[word] = word_freq_dict.get(word, 0)+1
    # word_freq = [(key, value) for key, value in word_freq_dict.items()]
    # word_freq = sorted(word_freq, key=lambda item: item[1], reverse=True)
    # print(word_freq)
    # df = pd.DataFrame(columns=['word', 'freq'])
    # df['word'] = [word for word, _ in word_freq]
    # df['freq'] = [freq for _, freq in word_freq]
    # df['is_it_safe'] = -1
    # df['describing_neighborhood'] = -1
    # df.to_csv(os.path.join(qr.root_path, qr.data_folder, 'really_short_adj_freq.csv'), index=False)

    df = pd.read_csv(os.path.join(qr.root_path, qr.data_folder, 'really_short_adj_freq.csv'))
    print(df)
    safety_words = set(df['word'][df['is_it_safe'].isin(['1', '2'])])
    print(safety_words)
    print('safety words count:', len(safety_words))

    query = """SELECT count (distinct property_id)
                FROM review
                WHERE review.review_text LIKE ANY (values {})
                ;
                """.format(qr.format_keywords_for_search(list(safety_words)))
    qr.airbnb_cursor.execute(query)
    results = qr.airbnb_cursor.fetchall()
    print(results)

    """
    def find_adp(sentence):
        adps = [token for token in sentence if token.pos_ == 'ADP']
        return adps

    def find_objs_after_adp(token):
        results = []
        for child in token.children:
            if child.dep_ in pose.OBJECTS or child.pos_ == 'NOUN':
                preps, adp_negated = pose.get_all_preps(child)
                if len(preps):
                    for prep in preps:
                        _, objs = pose.get_all_objs_with_adjectives(prep)
                        results.append(([child, prep] + objs, adp_negated))
                else:
                    results.append(([child], adp_negated))
        return results

    def get_objs_from_children(adp):
        results = find_objs_after_adp(adp)
        for objs, adp_negated in results:
            if adp_negated:
                return " ".join(['!', adp] + objs)
            else:
                return " ".join([adp.lower_] + [obj.lower_ for obj in objs])

    def get_pps(doc):
        "Function to get PPs from a parsed document."
        pps = []
        for token in doc:
            # Try this with other parts of speech for different subtrees.
            if token.pos_ == 'ADP':
                pp = ' '.join([tok.orth_ for tok in token.subtree])
                pps.append(pp)
        return pps

    sentence = 'there are a lot of restaurants are around the corner'
    pose = POSExtractor(qr.nlp(sentence))
    adps = find_adp(pose.sentence)
    extracted = []
    for adp in adps: # todo add a flag of visited to each adp
        pp = ' '.join([token.orth_ for token in adp.subtree])
        print(pp)

        
        child_part = get_objs_from_children(adp)
        print('child:', child_part)
        ancestors = adp.ancestors
        phrases = []
        for ancestor in ancestors:
            if ancestor.pos_ == 'NOUN':
                token = ancestor
                phrases.append(ancestor.lower_)
                for token_2 in list(token.lefts) + list(token.rights):
                    if token_2.dep_ in pose.ADJECTIVES or token_2.pos_ == 'ADJ':
                        more_adjs = [token_2]
                        more_adjs.extend(pose.get_adjs_from_conjunctions(more_adjs))
                        phrases.append(" ".join([item.lower_ for item in more_adjs] +
                                                [token.lower_]))
                    elif token_2.dep_ in pose.PREPOSITIONS:
                        _, objs = pose.get_all_objs(token_2)
                        for obj in objs:
                            for token_3 in list(obj.rights) + list(obj.lefts):
                                if token_3.pos_ == 'ADJ' or token_3.dep_ in pose.ADJECTIVES:
                                    more_adjs = [token_3]
                                    more_adjs.extend(pose.get_adjs_from_conjunctions(more_adjs))
                                    phrases.append([token.lower_, token_2.lower_] +
                                                   [item.lower_ for item in more_adjs] +
                                                   [obj.lower_])

            elif ancestor.pos_ == 'AUX':
                for token in ancestor.children:
                    if token.dep_ == 'attr' or token.dep_ in pose.SUBJECTS or token.pos_ == 'NOUN':
                        phrases.append(token.lower_)
                        for token_2 in list(token.lefts):
                            if token_2.dep_ in pose.ADJECTIVES or token_2.pos_ == 'ADJ':
                                more_adjs = [token_2]
                                more_adjs.extend(pose.get_adjs_from_conjunctions(more_adjs))
                                phrases.append(" ".join([item.lower_ for item in more_adjs] +
                                                        [token.lower_]))
                            elif token_2.dep_ in pose.PREPOSITIONS or token_2.pos_ == 'NOUN':
                                _, objs = pose.get_all_objs(token_2)
                                print('obj:', objs)
                                for obj in objs:
                                    for token_3 in obj.rights:
                                        if token_3.pos_ == 'ADJ' or token_3.dep_ in pose.ADJECTIVES:
                                            more_adjs = [token_3]
                                            more_adjs.extend(pose.get_adjs_from_conjunctions(more_adjs))
                                            phrases.append(" ".join([token.lower_, token_2.lower_] +
                                                           [item.lower_ for item in more_adjs] +
                                                           [obj.lower_]))
                                    for token_3 in obj.lefts:
                                        if token_3.pos_ == 'ADJ' or token_3.dep_ in pose.ADJECTIVES:
                                            more_adjs = [token_3]
                                            more_adjs.extend(pose.get_adjs_from_conjunctions(more_adjs))
                                            phrases.append(" ".join([token.lower_, token_2.lower_] +
                                                                    [item.lower_ for item in more_adjs] +
                                                                    [obj.lower_]))
                                                                    
        """


