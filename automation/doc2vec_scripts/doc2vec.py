import os
import csv
import json
import sys
import time
import logging
import datetime
import ast
from gensim.models import doc2vec
import urltools

if len(sys.argv) == 1:
    print "Provide year as a parameter"
    exit(0)

current_year = sys.argv[1]
if not current_year or '1996' > current_year:
    print "Invalid year input"
    exit(0)

properties = {}
f = open("config.properties", "r")
for line in f:
    parts = line.strip().split("=")
    if len(parts) < 2:
        continue
    properties[parts[0].strip()] = parts[1].strip()
f.close()

working_dir = properties.get("home_directory", "/dartfs-hpc/rc/lab/P/PhillipsG/experiments/wtnic/") + current_year + "/"
current_year = int(current_year)

log_filename = working_dir + "doc2vec.log"
logging.basicConfig(level=logging.DEBUG)
handler = logging.FileHandler(log_filename)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
log = logging.getLogger("doc2vec")
log.setLevel(logging.INFO)
log.addHandler(handler)


def normalize_url(company):
    web = company.replace("_", "/")
    if web.startswith("http://") or web.startswith("https://"):
        parse = urltools.parse(web)
    else:
        web = "http://" + web
        parse = urltools.parse(web)
    url = parse.domain + "." + parse.tld
    return url


class LabeledLineSentence(object):
    def __init__(self, final_subset, start_year, end_year):
        self.final_companies = set(final_subset)
        self.dnf = working_dir + "directory_not_found"
        if not os.path.isfile(self.dnf):
            f = open(self.dnf, "w")
            f.close()
        self.start_year = start_year
        self.end_year = end_year
        self.max_level = 3

    def __iter__(self):
        try:
            count = 0
            for company in self.final_companies:
                if not company:
                    continue
                count += 1
                word_list = self.generate_bag_of_words(company)
                if not word_list:
                    log.info("ERROR: no content for " + company)
                    continue
                word_list = word_list.split(" ")
                yield doc2vec.TaggedDocument(word_list, [company])

        except Exception as ex:
            print ex

    def generate_bag_of_words(self, company):
        directory = properties.get("data_directory", "/dartfs-hpc/rc/lab/P/PhillipsG/s3data/")
        url = normalize_url(company)
        dir = directory + url + "/"
        if not os.path.isdir(dir):
            f = open(self.dnf, "a")
            f.write(company + "\n")
            f.close()
            return
        years = os.listdir(dir)
        years = [int(x) for x in years]
        years.sort()

        words = ""
        file_count = 0
        for year in years:
            if self.start_year <= year <= self.end_year:
                s = time.time()
                y = dir + str(year) + "/"
                files = os.listdir(y)
                levels = []
                for filename in files:
                    if "txt" == filename[-3:]:
                        levels.append(int(filename[:-4]))
                levels.sort()
                for level in levels:
                    if level <= self.max_level:
                        path = y + str(level) + ".txt"
                        f = open(path, "r")
                        content = f.read().strip().lower().replace("__info__", " ")
                        words += content
                        f.close()
                        file_count += 1
        return words


class WTNICDoc2Vec(object):
    logger = logging.getLogger(__name__)

    def __init__(self, companies_list_filepath, year):
        self.corpus = []
        self.companies = set()
        self.public_firmids = {}
        self.totalSimsPrivate = []
        self.totalSimsPublic = []
        self.final_subset = []
        self.year = year
        gvkey_dir = properties.get("public_gvkeys_dir", "/dartfs-hpc/rc/lab/P/PhillipsG/experiments/wtnic"
                                                        "/master_files/public_companies_firmid_gvkeys_yearwise/")
        gvkey_filepath = os.path.join(gvkey_dir, str(year) + ".csv")
        with open(gvkey_filepath) as csvfile:
            cs = csv.reader(csvfile, delimiter='\t', quotechar='"')
            for c in list(cs)[1:]:
                self.public_firmids[c[0]] = c[1]

        naics_filepath = properties.get("naics_code_dict_filepath", "/dartfs-hpc/rc/lab/P/PhillipsG/experiments/wtnic"
                                                                    "/master_files/new_normalized_naics_code.txt")
        f = open(naics_filepath, "r")
        self.firmIds = ast.literal_eval(f.read().strip())
        f.close()

        self.final_subset = [x.strip() for x in open(companies_list_filepath, "r").readlines()]
        public_dir = properties.get("public_companies_list_dir", "/dartfs-hpc/rc/lab/P/PhillipsG/experiments/wtnic"
                                                                 "/master_files/yearwise_public_companies_list")
        public_companies_filepath = os.path.join(public_dir, str(year) + ".txt")
        self.final_subset.extend([x.strip() for x in open(public_companies_filepath, "r").readlines()])

        print "Pre-training tags size: ", len(self.final_subset)
        return

    def make_corpus(self):
        logging.info("Constructing corpus")
        self.corpus = LabeledLineSentence(self.final_subset, self.year - 4, self.year)
        labels = set()
        for doc in self.corpus:
            labels.add(doc.tags[0])
        logging.info("Corpus constructed with tags count: " + str(len(labels)))
        return

    def save_tags(self):
        filepath = os.path.join(working_dir, properties.get("training.post_model_tags_prefix", "post_training_tags_") + str(self.year) + ".txt")
        f = open(filepath, "w")
        for doc in self.corpus:
            label = doc.tags[0]
            f.write(label + "\n")
        f.close()

    def train_model(self, model_filename, dimensions):
        start = time.time()
        if os.path.isfile(model_filename):
            self.model = doc2vec.Doc2Vec.load(model_filename)
            print "Gensim Model loaded"
            logging.info("Modeling complete")
            message = "Time taken for " + model_filename + " is: " + str(time.time() - start)
            log.info(message)
            return

        logging.info("Constructing model")
        self.model = doc2vec.Doc2Vec(vector_size=dimensions, min_count=5, epochs=100, workers=16, hs=1, window=8)

        logging.info("Constructing vocab")
        self.model.build_vocab(self.corpus)

        logging.info("Training model")
        self.model.train(self.corpus, total_examples=self.model.corpus_count, epochs=self.model.iter)

        try:
            self.model.save(model_filename)
        except Exception as e:
            log.info("ERROR: Gensim Model saved failed - " + str(e))

        logging.info("Modeling complete")
        message = "Time taken for " + model_filename + " is: " + str(time.time() - start)
        log.info(message)

    def find_similar_public(self, top=37000):
        logging.info("Public - Computing similarities")
        lookup = 0
        sim_lookup = 0
        not_found = set()
        for label in self.corpus:
            label = label.tags[0].strip()
            if label is None or label not in self.public_firmids or label not in self.final_subset:
                continue
            if label not in self.public_firmids:
                not_found.add(label)
                continue

            s = time.time()
            doc_vec = self.model.docvecs[label]
            lookup += time.time() - s
            s = time.time()
            sims = self.model.docvecs.most_similar([doc_vec], topn=top)
            sim_lookup += time.time() - s
            for (docid, sim) in sims:
                if docid not in self.public_firmids or docid == label:
                    continue
                self.totalSimsPublic.append((label, docid, sim))
        logging.info("Public - Computing similarities complete")
        log.info("Public - Cumulative infer_vector call time: " + str(lookup))
        log.info("Public - Cumulative most_similar call time: " + str(sim_lookup))
        log.info("Public - Total not_found firmId labels: " + str(len(not_found)))

    def save_results_public(self, filepath):
        with open(filepath, 'w') as outfile:
            outfile.write("focal_firmid\trival_firmid\twtnic_score\n")
            for tup in self.totalSimsPublic:
                try:
                    outfile.write('{}\t{}\t{}'.format(self.public_firmids[tup[0]], self.public_firmids[tup[1]], tup[2]))
                    outfile.write("\n")
                except:
                    continue
        outfile.close()


def main(year):
    log.info("Doc2vec training started at: " + str(datetime.datetime.today()))
    dimension = int(properties.get("training.dimensions", 200))

    model_dir = working_dir + properties.get("training.model_dir_name", "model")
    public_peer_dir = working_dir + properties.get("training.public_peer_dir_name", "public_peer_dir")

    if not os.path.exists(model_dir):
        os.mkdir(model_dir)
    if not os.path.exists(public_peer_dir):
        os.mkdir(public_peer_dir)

    model_path = os.path.join(model_dir, properties.get("training.model_filename_prefix", "doc2vec_model_") + str(year))
    peer_filepath = os.path.join(public_peer_dir, properties.get("training.public_peers_filename_prefix",  "public_peers_file_") + str(year) + ".csv")
    companies_list_filepath = os.path.join(working_dir, properties.get("training.pre_model_tags_prefix", "pre_training_tags_") + str(year) + ".txt")

    if not os.path.isfile(companies_list_filepath):
        print "Training companies list file doesn't exist. Looking for: " + companies_list_filepath
        return

    d2v = WTNICDoc2Vec(companies_list_filepath, year)

    log.info("Model filename: " + model_path)
    start_time = time.time()
    d2v.make_corpus()
    d2v.train_model(model_path, dimension)
    message = "Time taken for " + model_path + " is: " + str(time.time() - start_time)
    stats_filepath = os.path.join(working_dir, properties.get("training.stats_filename_prefix", "training_stats_") + str(year) + ".txt")

    if os.path.isfile(stats_filepath):
        stat_file = open(stats_filepath, "a")
    else:
        stat_file = open(stats_filepath, "w")
    stat_file.write(message + "\n")
    stat_file.close()
    log.info(message)
    log.info("Datetime: " + str(datetime.datetime.today()))
    d2v.save_tags()

    log.info("Public Similarity filename: " + peer_filepath)
    start_time = time.time()
    d2v.find_similar_public()
    d2v.save_results_public(peer_filepath)
    message = "Time taken for " + peer_filepath + " is: " + str(time.time() - start_time)
    stat_file = open(stats_filepath, "a")
    stat_file.write(message + "\n")
    stat_file.close()
    log.info(message)
    log.info("Datetime: " + str(datetime.datetime.today()))


if __name__ == "__main__":
    main(current_year)
