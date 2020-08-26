from tqdm import tqdm
import pandas as pd
import os
# local import
from airbnb_disorder_analytics.analytics.query_reviews import QueryReview, POSExtractor

qr = QueryReview()
qr.batch_size = 10000

df = pd.read_csv(os.path.join(qr.root_path, qr.data_folder, 'really_short_adj_freq.csv'))
safe_words = set(df['word'][df['is_it_safe'].isin(['1'])])
unsafe_words = set(df['word'][df['is_it_safe'].isin(['2'])])
# pprint(safe_words)
# pprint(unsafe_words)

keywords = {'safe': ['safe', 'secure', 'safely', 'securely', 'secured', 'safety', 'security'],
            'safe_comparative': ['safer', 'safest'],
            'unsafe': ['sketch', 'sketchy', 'dangerous', 'insecure', 'dangerously', 'sketchiness'
                       'shaddy', 'dodgy', 'shabby', 'shoddy',
                       'shady', 'scary', 'scared',
                       'seedy', 'shoddy', 'questionable',
                       'iffy', 'fear', 'homeless', 'tricky'],
            'unsafe_comparative': ['sketchier', 'sketchiest']}  # determine if sentence mentions safety

# train a classifier to classify the sentiment of the sentence
# used labelled data --> a rule-based classifier, a combinator of words, and !

rule_based_matching = []
# positive -- double positive
rule_based_matching += [(keyword, False, 1) for keyword in keywords['safe']]
# positive -- double negative
rule_based_matching += [(keyword, True, 1) for keyword in keywords['unsafe']]
# positive - comparative forms
rule_based_matching += [('safer', False, 1)]
rule_based_matching += [('safer', True, 1)]
rule_based_matching += [('safest', True, 1)]
# negative - comparative forms
rule_based_matching += [('safest', False, 0)]
# negative - double negative
rule_based_matching += [(keyword, False, 0) for keyword in keywords['unsafe']]
# negative - negated positive
rule_based_matching += [(keyword, True, 0) for keyword in keywords['safe']]

while True:
    query = """SELECT review.reviewer_id, review.property_id, review.review_text
                   FROM review, property
                   WHERE review.review_text LIKE ANY (values {})
                   AND review.property_id = property.airbnb_property_id
                   AND property.state = 'New York'
                   AND review.positive_neighbor = -1
                   LIMIT {}
                   ;
                   """.format(qr.format_keywords_for_search(qr.list_of_nouns_for_neighborhood), qr.batch_size)
    qr.airbnb_cursor.execute(query)
    results = qr.airbnb_cursor.fetchall()
    if len(results) == 0:
        break

    training_raw_data = []
    for result in tqdm(results, total=len(results)):
        reviewer_id = result[0]
        property_id = result[1]
        review_text = result[2]
        # split text into sentence and get the one that has the nouns
        sentences = qr.get_sentence_with_keywords(review_text)
        list_of_adjs = []
        sentiment_flags = []
        for sentence in sentences:
            pose = POSExtractor(sentence)
            adjs = pose.get_all_adjs_for_keyword()
            list_of_adjs += adjs
            list_of_adjs += pose.get_svaos_for_keyword()
        for adj in list_of_adjs:
            negation_flag = False
            if adj[0] == '!':
                negation_flag = True
            # remove non-alphabetic chars from the end of string
            cleaned_adj = adj
            while len(cleaned_adj) and not cleaned_adj[-1].isalpha(): cleaned_adj = cleaned_adj[:-1]
            while len(cleaned_adj) and not cleaned_adj[0].isalpha(): cleaned_adj = cleaned_adj[1:]
            token = qr.nlp(cleaned_adj)
            for word in token:
                for keyword, negation, sentiment_flag in rule_based_matching:
                    if negation == negation_flag and word.lower_ == keyword:
                        sentiment_flags.append(sentiment_flag)
        if len(set(sentiment_flags)) == 1:
            final_sentiment_flag = int(sentiment_flags[0])*2-1
        elif len(set(sentiment_flags)) > 1:  # in the case of conflicting flags from the review --> return the negative flag
            final_sentiment_flag = -1
        else:
            final_sentiment_flag = 0
        query = """UPDATE review
                    SET really_short_adjs_for_neighbor = %s,
                    negative_neighbor = %s,
                    positive_neighbor = %s
                    WHERE reviewer_id = %s
                    AND property_id = %s
                    """
        qr.airbnb_cursor.execute(query, (','.join(list_of_adjs), int(final_sentiment_flag == -1),
                                         int(final_sentiment_flag == 1), reviewer_id, property_id))
        qr.airbnb_connection.commit()
