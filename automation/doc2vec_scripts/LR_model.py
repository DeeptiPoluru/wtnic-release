import pandas as pd
import glob,os
import json
import time
import collections
from sklearn import linear_model
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


def LR_sci_kit(input_dir, out_file):
    os.chdir(input_dir)
    out_json = open(out_file, "w+")
    rsq_arr = []
    result_dict = collections.defaultdict(list)
    for file in glob.glob("*.csv"):
        d = collections.OrderedDict()
        data = pd.read_csv(file, sep="\t", index_col=False, header=0)
        clf = linear_model.LinearRegression()

        x,y = data[['rival_stock_return', 'monopolist']], data.focal_stock_return
        clf.fit(x,y)
        r_value_stock = clf.score(x,y)
        print r_value_stock

        x, y = data[['rival_profit_sales', 'monopolist']], data.focal_profit_sales
        clf.fit(x, y)
        r_value_profit_sales = clf.score(x, y)
        print r_value_profit_sales

        x, y = data[['rival_profit_assets', 'monopolist']], data.focal_profit_assets
        clf.fit(x, y)
        r_profit_assets = clf.score(x, y)
        print r_profit_assets

        x, y = data[['rival_valuation', 'monopolist']], data.focal_valuation
        clf.fit(x, y)
        r_value_valuation = clf.score(x, y)
        print r_value_valuation

        avg_r_squared = (float(r_value_profit_sales) + float(r_profit_assets)) / 2
        d["no_of_docs"] = str(len(data.focal_firmid.unique()))
        d["input_file_name"] = file
        d["date_run"] = time.strftime("%c")
        d["similarity"] = file.split('_', 1)[0].replace('.', '').upper()
        d["type"] = "WTNIC"
        d["pruned_words_cutoff"] = "1-25%"
        d["WTNIC_Score_Cutoff"] = "98%"
        d["RSQ_average"] = avg_r_squared
        d["RSQ_stock_return"] = float(r_value_stock)
        d["RSQ_profit_sales"] = float(r_profit_assets)
        d["RSQ_profit_asset"] = float(r_profit_assets)
        d["RSQ_valuation"] = float(r_value_valuation)
        rsq_arr.append(d)
    result_dict["scores"] = rsq_arr
    json.dump(result_dict, out_json, indent=4)


if __name__ == "__main__":
    input_folder = os.path.join(working_dir, properties.get("evaluation.public_LR_dir", "public_LR_dir"))
    if not os.path.isdir(input_folder):
        print "Input folder not found!"
        exit(0)

    filename = properties.get("evaluation.public_result_filename_prefix", "Rsquare_output_") + str(current_year) + ".json"
    LR_sci_kit(input_folder, filename)
