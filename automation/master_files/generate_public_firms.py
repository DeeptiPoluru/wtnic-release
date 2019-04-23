import urltools
import csv
import ast
import os


def normalize_url(company):
    web = company.replace("_", "/")
    if web.startswith("http://") or web.startswith("https://"):
        parse = urltools.parse(web)
    else:
        web = "http://" + web
        parse = urltools.parse(web)
    path = parse.path.replace("/", "_")
    url = parse.domain + "." + parse.tld + path
    return url


def normalize_company_url(company_url):
    return company_url.replace("https://", "") \
        .replace("http://", "") \
        .replace(":", "_") \
        .replace("/", "_") \
        .replace("?", "_")


f = open("company_firmid_dict.txt", "r")
naics_code = ast.literal_eval(f.read())
f.close()

yearwise_gvkeys = {}
with open("PubicFirm_URLs_1995_2017_All.txt") as csvfile:
    cs = csv.reader(csvfile, delimiter='\t', quotechar='"')
    for c in list(cs)[1:]:
        year = int(c[4])
        if year not in yearwise_gvkeys:
            d = {normalize_url(c[9]):c[1]}
            yearwise_gvkeys[year] = d
        else:
            d = yearwise_gvkeys[year]
            d[normalize_url(c[9])] = c[1]

print "Total years: ", len(yearwise_gvkeys)

directory = "yearwise_public_companies_list/"
directory1 = "public_companies_firmid_gvkeys_yearwise/"

if not os.path.isdir(directory):
    os.mkdir(directory)

if not os.path.isdir(directory1):
    os.mkdir(directory1)

ywc = open("year_wise_public_companies_count.txt", "w")
compiled = open("public_companies_firmid_gvkey_compiled.csv", "w")
compiled.write("company\tfirm_id\tgvkey\tyear\n")

for year, value in yearwise_gvkeys.iteritems():
    path = directory + str(year) + ".txt"
    path1 = directory1 + str(year) + ".csv"
    f = open(path, "w")
    f1 = open(path1, "w")
    count = 0
    f1.write("company\tfirm_id\tgvkey\n")
    for company, gvkey in value.iteritems():
        datapath = "/dartfs-hpc/rc/lab/P/PhillipsG/s3data/" + company
        if company in naics_code and os.path.isdir(datapath):
            count += 1
            f.write(company + "\n")
            firmid = naics_code[company][0]
            f1.write(company + "\t" + str(firmid) + "\t" + str(gvkey) + "\n")
            compiled.write(company + "\t" + str(firmid) + "\t" + str(gvkey) + "\t" + str(year) + "\n")
    f1.close()
    f.close()
    ywc.write(str(year) + "\t" + str(count) + "\n")

ywc.close()
compiled.close()
