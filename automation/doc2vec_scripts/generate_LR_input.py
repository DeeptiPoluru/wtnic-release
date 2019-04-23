import csv, os
import pandas as pd
import logging
import urltools
import sys

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

logging.basicConfig(filename=working_dir + "lr_input_log.log", level=logging.INFO)
metric_stats = {}


def normalize_url(company):
    web = company.replace("_", "/")
    if web.startswith("http://") or web.startswith("https://"):
        parse = urltools.parse(web)
    else:
        web = "http://" + web
        parse = urltools.parse(web)
    url = parse.domain + "." + parse.tld
    return url


def map_company_gvkey():
    public_firmids = {}
    gvkey_dir = properties.get("public_gvkeys_dir", "/dartfs-hpc/rc/lab/P/PhillipsG/experiments/wtnic"
                                                    "/master_files/public_companies_firmid_gvkeys_yearwise/")
    gvkey_filepath = os.path.join(gvkey_dir, str(current_year) + ".csv")
    with open(gvkey_filepath) as csvfile:
        cs = csv.reader(csvfile, delimiter='\t', quotechar='"')
        for c in list(cs)[1:]:
            public_firmids[c[0]] = c[1]

    return public_firmids


def process_file(filepath, filename, out_dir):
    public_firmids = map_company_gvkey()
    total_file_count = len(public_firmids)
    logging.info("Gvkey url length: " + str(total_file_count))

    data = pd.read_csv(filepath, sep="\t")
    # ufc_csv => set of gvkeys of unique focal companies read from csv file
    ufc_csv = set(data.focal_firmid.unique())
    logging.info("Data length read from the input file: " + filepath + " - " + str(len(data)))
    logging.info("Unique focal companies read from file: " + str(len(ufc_csv)))
    # ufc_bco => set of gvkeys of unique focal companies before cut off applied to data
    ufc_bco = ufc_csv

    required_top_peers = int((total_file_count * (total_file_count - 1)) * 0.02)
    data = data.sort_values('wtnic_score', ascending=False)
    data = data.reset_index(drop=True)
    data = data.head(required_top_peers)

    print("Data length after filtering: " + str(len(data)))
    logging.info("After input threshold cut off, the filtered data length is: " + str(len(data)))
    print ("Generating rivalry companies report ")

    # ufc_aco => set of gvekeys of unique focal companies after cut off applied to the data set
    ufc_aco = set(data.focal_firmid.unique())
    logging.info("Unique focal companies after cut off applied to dataset: " + str(len(ufc_aco)))

    # merge metric stats with company data
    profit_assets = []
    profit_sales = []
    stock_return = []
    valuation = []
    ignored_focal_keys = ufc_bco - ufc_aco

    for k in data['rival_firmid']:
        profit_assets.append(metric_stats[k]["profit_assets"])
        profit_sales.append(metric_stats[k]["profit_sales"])
        stock_return.append(metric_stats[k]["stock_return"])
        valuation.append(metric_stats[k]["valuation"])
    
    data['rival_profit_assets'] = profit_assets
    data['rival_profit_sales'] = profit_sales
    data['rival_stock_return'] = stock_return
    data['rival_valuation'] = valuation

    print "Data length %s" % str(len(data))

    rival_avg = data.groupby('focal_firmid')[
        'rival_profit_assets', 'rival_profit_sales', 'rival_stock_return', 'rival_valuation'].mean()

    profit_assets = []
    profit_sales = []
    stock_return = []
    valuation = []
    monopolist = []
    for index, row in rival_avg.iterrows():

        profit_assets.append(metric_stats[index]["profit_assets"])
        profit_sales.append(metric_stats[index]["profit_sales"])
        stock_return.append(metric_stats[index]["stock_return"])
        valuation.append(metric_stats[index]["valuation"])
        monopolist.append("0")
    
    rival_avg['focal_profit_assets'] = profit_assets
    rival_avg['focal_profit_sales'] = profit_sales
    rival_avg['focal_stock_return'] = stock_return
    rival_avg['focal_valuation'] = valuation
    rival_avg['monopolist'] = monopolist

    logging.info("Final number of focal companies:" + str(len(rival_avg)))
    # write dataframe to csv
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    outfile = out_dir + "/" + filename[:-4] + "_LR.csv"
    rival_avg.to_csv(outfile, sep='\t', encoding='utf-8')

    # add monopolist column and its value and add all the ignored focal_keys in the file to generate the LR_input file
    out_json = open(outfile, "a")
    for key in ignored_focal_keys:
        out_json.write(str(key) + "\t" + "0.0\t0.0\t0.0\t0.0\t" + str(metric_stats[key]["profit_assets"]) + "\t" + str(
            metric_stats[key]["profit_sales"]) + "\t" + str(metric_stats[key]["stock_return"]) + "\t" + str(
            metric_stats[key]["valuation"]) + "\t1\n")
    out_json.close()


def LR_input_main(input_directory, output_directory):
    gvkey_dir = properties.get("public_gvkeys_dir", "/dartfs-hpc/rc/lab/P/PhillipsG/experiments/wtnic"
                                                    "/master_files/public_companies_firmid_gvkeys_yearwise/")
    filename = os.path.join(gvkey_dir, str(current_year) + ".csv")
    counter = 0
    lines = list(csv.reader(open(filename, 'r'), delimiter='\t'))
    for line in lines:
        counter += 1
        if counter <= 1:
            continue
        firmid = int(line[1])
        metric_stats[firmid] = {}
        metric_stats[firmid]["company_url"] = normalize_url(line[0])
        metric_stats[firmid]["profit_assets"] = float(line[3])
        metric_stats[firmid]["profit_sales"] = float(line[4])
        metric_stats[firmid]["stock_return"] = float(line[5])
        metric_stats[firmid]["valuation"] = float(line[6])

    logging.info("Total " + str(len(metric_stats)) + " lines parsed from GVKEY file.")
    count = 0
    for dirpath, dirnames, filenames in os.walk(input_directory):
        for filename in filenames:
            print filename
            if filename.endswith(".csv"):
                filepath = os.path.join(dirpath, filename)
                count += 1
                process_file(filepath, filename, output_directory)


if __name__ == "__main__":
    public_peer_dir = working_dir + properties.get("training.public_peer_dir_name", "public_peer_dir")
    logging.info("Generating LR input for Doc2vec peer file")
    output_dir = working_dir + properties.get("evaluation.public_LR_dir", "public_LR_dir")
    LR_input_main(public_peer_dir, output_dir)
