import random
import sys
import os

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

if not os.path.exists(working_dir):
    os.mkdir(working_dir)

f = open(os.path.join(properties.get("private_companies_list_dir"), current_year + ".txt"), "r")
pool = set()
for line in f:
    pool.add(line.strip())
f.close()
print "total: ", len(pool)

filepath = os.path.join(working_dir, properties.get("training.pre_model_tags_prefix", "pre_training_tags_") + str(current_year) + ".txt")
f = open(filepath, "w")
sample = random.sample(pool, 32000)
print "sample: ", len(sample)
for s in sample:
    f.write(s + "\n")
f.close()
