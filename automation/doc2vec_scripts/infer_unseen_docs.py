import multiprocessing
import sys
import time
import os
from gensim.models import doc2vec
from gensim.models.keyedvectors import Word2VecKeyedVectors
import logging
import ast
import urltools

if len(sys.argv) == 1:
    print "Provide year as a parameter"
    exit(0)

current_year = sys.argv[1]
if not current_year or '1996' > current_year:
    print "Invalid year input"
    exit(0)

threshold = 0.22
if len(sys.argv) > 2 and sys.argv[2]:
    threshold = float(sys.argv[2])

top = 15.0
if len(sys.argv) > 3 and sys.argv[3]:
    top = float(sys.argv[3])

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

handler = logging.FileHandler(working_dir + "unseen.log")
log = logging.getLogger("unseen")
log.setLevel(logging.INFO)
log.addHandler(handler)

start_time = time.time()

if os.path.isfile("directory_not_found"):
    f = open("directory_not_found", "w")
    f.write("\n")
    f.close()


def normalize_company_name(company_url):
    return company_url.replace("https://", "") \
        .replace("http://", "") \
        .replace(":", "_") \
        .replace("/", "_") \
        .replace("?", "_")


firm_id_dict_filepath = properties["firm_ids_dict_filepath"]
f = open(firm_id_dict_filepath, "r")
firmIds = ast.literal_eval(f.read().strip())
f.close()

model_dir = working_dir + properties.get("training.model_dir_name", "model")
if not os.path.isdir(model_dir):
    print "Model directory not found"
    exit(0)

model_filepath = os.path.join(model_dir, properties["training.model_filename_prefix"] + str(current_year))
model = None

s = time.time()

if os.path.isfile(model_filepath):
    model = doc2vec.Doc2Vec.load(model_filepath)
    print ("Model loaded"),
else:
    print ("Model not found")
    exit(0)

model_load_time = time.time() - s
print (model_load_time)

temp_vocab = multiprocessing.Manager().dict()
time_count = multiprocessing.Manager().dict()
time_count["total_bow_time"] = 0
time_count["total_infer_time"] = 0

print ("Vector size: ", model.vector_size)


def generate_bag_of_words(company):
    max_level = 3
    start_year, end_year = current_year - 4, current_year
    dnf = working_dir + "directory_not_found"
    directory = properties.get("data_directory")
    web = company.replace("_", "/")
    if web.startswith("http://") or web.startswith("https://"):
        parse = urltools.parse(web)
    else:
        web = "http://" + web
        parse = urltools.parse(web)
    url = parse.domain + "." + parse.tld
    dir = directory + url + "/"
    if not os.path.isdir(dir):
        f = open(dnf, "a")
        f.write(company + "\n")
        f.close()
        return
    years = os.listdir(dir)
    years = [int(x) for x in years]
    years.sort()

    words = ""
    file_count = 0
    for year in years:
        if start_year <= year <= end_year:
            y = dir + str(year) + "/"
            files = os.listdir(y)
            levels = []
            for filename in files:
                if "txt" == filename[-3:]:
                    levels.append(int(filename[:-4]))
            levels.sort()
            for level in levels:
                if level <= max_level:
                    path = y + str(level) + ".txt"
                    f = open(path, "r")
                    content = f.read().strip().lower().replace("__info__", " ")
                    words += content
                    f.close()
                    file_count += 1
    return words


def infer_company(url, lock):
    key = url.strip()
    if key == "" or key not in firmIds:
        return

    if key in model.docvecs:
        s = time.time()
        vector = model.docvecs[key]
        t = time.time() - s
        time_count["total_infer_time"] += t
        temp_vocab[key] = vector
        log.info(key + ",1,0,0," + str(t) + "," + str(len(temp_vocab)))
        return

    s = time.time()
    words = generate_bag_of_words(key)
    t1 = time.time() - s
    if not words:
        return
    time_count["total_bow_time"] += t1
    words = words.split(" ")
    if len(words) < 250:
        return
    s = time.time()
    vector = model.infer_vector(words)
    t2 = time.time() - s
    time_count["total_infer_time"] += t2
    temp_vocab[key] = vector
    log.info(key + ",0," + str(len(words)) + "," + str(t1) + "," + str(t2) + "," + str(len(temp_vocab)))


def create_processes(temp):
    jobs = []
    try:
        for c in temp:
            p = multiprocessing.Process(target=infer_company, args=(c, 1))
            jobs.append(p)

        for job in jobs:
            job.start()

    except Exception as e:
        print (e)
        print (type(temp), len(temp))


def write_companies(vocab):
    companies = set()
    count = 0
    filepath = os.path.join(working_dir,
                            properties["evaluation.infer_tags_filename_prefix"] + str(current_year) + ".txt")
    f = open(filepath, "w")
    for c in vocab.index2entity:
        companies.add(c)
        f.write(c + "\n")
        count += 1
    print ("vocab entities size: ", len(companies), count)
    f.close()
    return companies


def load_companies():
    filepath = os.path.join(working_dir,
                            properties["evaluation.infer_tags_filename_prefix"] + str(current_year) + ".txt")
    if not os.path.isfile(filepath):
        return None
    companies = set()
    f = open(filepath, "r")
    for c in f.readlines():
        c = c.strip()
        companies.add(c)
    f.close()
    return companies


def load_vocab(vocab, total_time=0):
    good = dict(temp_vocab)
    for key, value in good.iteritems():
        vocab.add(entities=[key], weights=[value])
    good.clear()


def infer_private_companies(alpha=-1, topn=top):
    vocab_file = working_dir + properties["evaluation.private_vocab_filename_prefix"] + str(current_year)
    peer_dir = working_dir + properties["training.private_peer_dir_name"]
    if not os.path.isdir(peer_dir):
        os.mkdir(peer_dir)

    peer_filepath = peer_dir + "/" + \
                    properties["training.private_peers_filename_prefix"] + str(current_year) + ".csv"

    report_filepath = os.path.join(working_dir, properties["evaluation.infer_report_filename_prefix"] + str(current_year) + ".txt")
    if not os.path.isfile(report_filepath):
        f = open(report_filepath, "w")
        f.close()

    report = open(report_filepath, "a")
    report.write("Model filepath: " + model_filepath + "\n")
    report.write("Model load time: " + str(model_load_time) + "\n")
    report.write("Result peer file: " + peer_filepath + "\n")
    report.close()

    if not os.path.isfile(vocab_file):
        vocab = Word2VecKeyedVectors(vector_size=model.vector_size)

        f = open(os.path.join(properties["private_companies_list_dir"], str(current_year) + ".txt"), "r")
        companies_list = f.readlines()
        f.close()

        start = time.time()
        start_index = 0
        while start_index < len(companies_list):
            create_processes(list(companies_list[start_index:start_index + 16]))
            start_index += 16

        s = time.time()
        load_vocab(vocab)
        lt = time.time() - s

        print ("Vocab size: " + str(len(vocab.index2entity)))

        good_time_counts = dict(time_count)
        report = open(report_filepath, "a")
        report.write("Time taken to generate bag of words: " + str(good_time_counts["total_bow_time"]) + "\n")
        report.write("Time taken to infer vectors: " + str(good_time_counts["total_infer_time"]) + "\n")
        report.write("Time taken to load vectors: " + str(lt) + "\n")
        report.write("Time taken to build vocab: " + str(time.time() - start) + "\n")
        s = time.time()
        vocab.save(vocab_file)
        report.write("Time taken to save vocab file: " + str(time.time() - s) + "\n")
        report.close()
        print ("KeyedVectors stored succesfully.")
    else:
        start = time.time()
        s = time.time()
        vocab = Word2VecKeyedVectors.load(vocab_file, mmap="r")
        report = open(report_filepath, "a")
        report.write("Time taken to load vocab: " + str(time.time() - s) + "\n")
        report.close()

    companies = write_companies(vocab)

    s = time.time()
    top = int(len(companies) * topn / 100)

    count = 0
    if os.path.isfile(peer_filepath):
        report = open(report_filepath, "a")
        report.write("Peer file already present.\n")
        report.close()
        return
    result = open(peer_filepath, "w")
    result.write("focal_firmid\trival_firmid\twtnic_score\n")
    for company in companies:
        if company not in firmIds:
            continue
        focal_key = firmIds[company][0]
        sims = vocab.most_similar(positive=[company], topn=top)
        for (rival_company, score) in sims:
            if rival_company not in firmIds or company == rival_company:
                continue
            if score < threshold:
                continue
            rival_key = firmIds[rival_company][0]
            result.write(str(focal_key) + "\t" + str(rival_key) + "\t" + str(score) + "\n")
        count += 1
        if count % 100 == 0:
            print ("Similarity done for: ", count)

    result.close()
    report = open(report_filepath, "a")
    report.write("Peer file generation time: " + str(time.time() - s) + "\n")
    report.write("Time taken: " + str(time.time() - start) + "\n\n")
    report.close()


if __name__ == "__main__":
    infer_private_companies()
    print ("Time taken: ", time.time() - start_time)
