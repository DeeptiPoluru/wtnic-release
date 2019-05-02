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
firmIds = ast.literal_eval(f.read())
f.close()

yearwise_gvkeys = {}
with open("PubicFirm_URLs_1995_2017_All.txt") as csvfile:
    cs = csv.reader(csvfile, delimiter='\t', quotechar='"')
    for c in list(cs)[1:]:
        year = int(c[4])
        if year not in yearwise_gvkeys:
            # url -> (gvkey, profit_assets, profit_sales, stock_return, valuation)
            d = {normalize_url(c[9]): (c[1], c[10], c[11], c[12], c[13])}

            yearwise_gvkeys[year] = d
        else:
            d = yearwise_gvkeys[year]
            d[normalize_url(c[9])] = (c[1], c[10], c[11], c[12], c[13])

print "Total years: ", len(yearwise_gvkeys)

directory = "public_companies_firmid_gvkeys_yearwise/"

if not os.path.isdir(directory):
    os.mkdir(directory)

for year, value in yearwise_gvkeys.iteritems():
    path = directory + str(year) + ".csv"
    f = open(path, "w")
    count = 0
    f.write("company\tfirm_id\tgvkey\tprofit_assets\tprofit_sales\tstock_return\tvaluation\n")
    for company, items in value.iteritems():
        datapath = "/dartfs-hpc/rc/lab/P/PhillipsG/s3data/" + company
        if company in firmIds and os.path.isdir(datapath):
            firmid = firmIds[company][0]
            f.write(company + "\t" + str(firmid) + "\t" + str(items[0]) + "\t" + str(items[1]) + "\t" +
                    str(items[2]) + "\t" + str(items[3]) + "\t" + str(items[4]) + "\n")
    f.close()


'''
required fields

profit_assets
profit_sales
stock_return
valuation
'''