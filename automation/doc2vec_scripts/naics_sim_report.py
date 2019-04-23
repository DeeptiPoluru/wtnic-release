import pandas as pd
import glob
import time
import ast
import os
import logging
import csv
import sys
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

logging.basicConfig(filename=working_dir + "naics_report_log.log", level=logging.INFO, format='%(asctime)s %(message)s')
start = time.time()

f = open(properties["naics_code_dict_filepath"], "r")
naics_code = ast.literal_eval(f.read())
f.close()

gvkey_filepath = os.path.join(properties["public_gvkeys_dir"], str(current_year) + ".csv")

f = open(properties["firmid_naics_mapping_dict_filepath"], "r")
firmid_naics = ast.literal_eval(f.read())
f.close()

yearwise_private_count = {}
f = open(properties["private_yearwise_count_filepath"], "r")
for line in f:
    line = line.strip().split("\t")
    if len(line) < 2:
        continue
    yearwise_private_count[int(line[0])] = int(line[1])


def valid_naics(code):
    return (6 - len(code)) * "0" + code


def normalize_url(company):
    web = company.replace("_", "/")
    if web.startswith("http://") or web.startswith("https://"):
        parse = urltools.parse(web)
    else:
        web = "http://" + web
        parse = urltools.parse(web)
    url = parse.domain + "." + parse.tld
    return url


def pre_processing(filename, total_comapnies):
    if not os.path.isfile(filename):
        print "File: " + filename + " not found."
        return None
    f = open(filename, "r")
    not_found = set()
    company_6_naics, naics_6_frequency = dict(), dict()
    company_5_naics, naics_5_frequency = dict(), dict()
    company_4_naics, naics_4_frequency = dict(), dict()
    company_3_naics, naics_3_frequency = dict(), dict()
    company_2_naics, naics_2_frequency = dict(), dict()

    for line in f:
        company = normalize_url(line.strip())
        if company in naics_code:
            naics = naics_code[company][1]
            if "" != naics:
                naics = valid_naics(naics)
            else:
                not_found.add(line)
                continue
            n6, n5, n4, n3, n2 = naics, naics[:5], naics[:4], naics[:3], naics[:2]

            company_2_naics[company] = n2
            if n2 in naics_2_frequency:
                naics_2_frequency[n2] += 1
            else:
                naics_2_frequency[n2] = 1

            company_3_naics[company] = n3
            if n3 in naics_3_frequency:
                naics_3_frequency[n3] += 1
            else:
                naics_3_frequency[n3] = 1

            company_4_naics[company] = n4
            if n4 in naics_4_frequency:
                naics_4_frequency[n4] += 1
            else:
                naics_4_frequency[n4] = 1

            company_5_naics[company] = n5
            if n5 in naics_5_frequency:
                naics_5_frequency[n5] += 1
            else:
                naics_5_frequency[n5] = 1

            company_6_naics[company] = n6
            if n6 in naics_6_frequency:
                naics_6_frequency[n6] += 1
            else:
                naics_6_frequency[n6] = 1
        else:
            not_found.add(line.strip())
            continue

    f.close()

    not_found_size = len(not_found)
    if not_found_size > 0:
        logging.error("ERROR: Total companies for which naics code not found: " + str(not_found_size))
    if 0 != not_found_size:
        logging.error(repr(not_found))
    total_comapnies -= not_found_size
    # 2/3 - digit naics code -> frequency, frequency*(frequency-1)
    v2, v3, v4, v5, v6, required_top_peers2, required_top_peers3, required_top_peers4, required_top_peers5, \
        required_top_peers6 = 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    for key, value in naics_2_frequency.iteritems():
        factor = value * (value - 1)
        naics_2_frequency[key] = (value, factor)
        v2 += value
        required_top_peers2 += factor
    for key, value in naics_3_frequency.iteritems():
        factor = value * (value - 1)
        naics_3_frequency[key] = (value, factor)
        v3 += value
        required_top_peers3 += factor
    for key, value in naics_4_frequency.iteritems():
        factor = value * (value - 1)
        naics_4_frequency[key] = (value, factor)
        v4 += value
        required_top_peers4 += factor
    for key, value in naics_5_frequency.iteritems():
        factor = value * (value - 1)
        naics_5_frequency[key] = (value, factor)
        v5 += value
        required_top_peers5 += factor
    for key, value in naics_6_frequency.iteritems():
        factor = value * (value - 1)
        naics_6_frequency[key] = (value, factor)
        v6 += value
        required_top_peers6 += factor
    if v2 != total_comapnies | v3 != total_comapnies | v4 != total_comapnies | v5 != total_comapnies | v6 != total_comapnies:
        print ("ERROR! Calculation doesn't match. v2: " + str(v2) + " and v3: " + str(v3) + " and v4: " + str(v4) + " and v5: " + str(v5) + " and v6: " + str(v6))

    return required_top_peers2, required_top_peers3, required_top_peers4, required_top_peers5, required_top_peers6


filenames = glob.glob(working_dir + properties["training.private_peer_dir_name"] + "/*.csv")
print filenames
for filepath in filenames:
    start_time = time.time()
    print "filepath: ", filepath
    total_firms = yearwise_private_count[current_year]
    filename = os.path.join(working_dir,
                            properties["evaluation.infer_tags_filename_prefix"] + str(current_year) + ".txt")
    s = time.time()
    x = pre_processing(filename, total_firms)
    print "pre-processing ", time.time() - s
    if None == x:
        logging.error("Error: in pre-processing for filepath: " + filepath)
        continue

    required_top_peers2, required_top_peers3, required_top_peers4, required_top_peers5, required_top_peers6 = x[0], x[1], x[2], x[3], x[4]

    rp = [required_top_peers2, required_top_peers3, required_top_peers4, required_top_peers5, required_top_peers6]
    peer_file = open(filepath, "r")

    data_len = int(total_firms * (total_firms - 1) * 0.02)
    required_top_peers2 = min(data_len, required_top_peers2)
    required_top_peers3 = min(data_len, required_top_peers3)
    required_top_peers4 = min(data_len, required_top_peers4)
    required_top_peers5 = min(data_len, required_top_peers5)
    required_top_peers6 = min(data_len, required_top_peers6)
    high = max(required_top_peers2, required_top_peers3, required_top_peers4, required_top_peers5, required_top_peers6)

    p = [required_top_peers2, required_top_peers3, required_top_peers4, required_top_peers5, required_top_peers6]
    similar_count = [0, 0, 0, 0, 0]
    count = 0

    firstLine = True
    for line in peer_file:
        line = line.strip().split()
        if firstLine:
            firstLine = False
            continue
        if len(line) < 3:
            break
        if high == 0:
            break
        focal_firmid = int(line[1])
        rival_firmid = int(line[2])
        if focal_firmid not in firmid_naics or rival_firmid not in rival_firmid:
            for i in xrange(len(p)):
                p[i] = p[i] - 1 if p[i] > 0 else p[i]
            high -= 1
            continue

        focal_naics = firmid_naics[focal_firmid]
        rival_naics = firmid_naics[rival_firmid]
        if not focal_naics or not rival_naics:
            for i in xrange(len(p)):
                p[i] = p[i] - 1 if p[i] > 0 else p[i]
            high -= 1
            continue

        count += 1
        i = 6
        while i >= 2:
            focal_naics = focal_naics[:i]
            rival_naics = rival_naics[:i]
            if focal_naics == rival_naics:
                j = i - 2
                while j >= 0:
                    if p[j] > 0:
                        similar_count[j] += 1
                        p[j] -= 1
                    j -= 1
                break
            i -= 1

    similar_scores = [0, 0, 0, 0, 0]
    for i in xrange(len(similar_count)):
        similar_scores[i] = float(similar_count[i]) / rp[i]

    report_filepath = os.path.join(working_dir, properties["evaluation.naics_report_filename_prefix"] + str(current_year) + ".txt")
    if os.path.isfile(report_filepath):
        report = open(report_filepath, "a")
    else:
        report = open(report_filepath, "w")

    report.write(filepath + "\t" + "Total firms: " + str(total_firms) + "\n")
    report.write("Company listing file: " + filename + "\n")
    report.write("Required top peers for 2-digit: " + str(required_top_peers2) + "; for 3-digit: " + str(required_top_peers3)
                 + "; for 4-digit: " + str(required_top_peers4) + "; for 5-digit: " + str(required_top_peers5) +
                 "; for 6-digit: " + str(required_top_peers6) + "\n")
    report.write("Found both naics code for: " + str(count) + "\n")
    report.write("2-digit-similarity: " + str(similar_scores[0]) + "\t" + "3-digit-similarity: " + str(similar_scores[1]) +
                 "\t" + "4-digit-similarity: " + str(similar_scores[2]) + "\t" + "5-digit-similarity: " + str(similar_scores[3]) +
                 "\t" + "6-digit-similarity: " + str(similar_scores[4]) + "\n")
    report.write("Time taken for this process: " + str(time.time() - start_time) + "\n\n")
    logging.info("Time taken to execute " + filepath + " is: " + str(time.time() - start))
    report.close()


