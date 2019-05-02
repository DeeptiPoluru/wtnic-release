import urltools
import ast

f = open('URL_id_file.txt', "r")
lineCount = 0
master = {}
#name, [id, {years}]
yearWiseCompanies = {}
#year, {company_url}
for line in f:
    line = line.strip()
    parts = line.split("\t")
    if len(parts) < 3:
        continue
    id, company, year = int(parts[0]), parts[1], int(parts[2])
    lineCount += 1
    if company in master:
        master[company][1].add(year)
    else:
        master[parts[1]] = [id, {year}]

    if year in yearWiseCompanies:
        yearWiseCompanies[year].add(company)
    else:
        yearWiseCompanies[year] = {company}

f.close()

print "Total lines: ", lineCount #9477100
print "Size of master: ", len(master) #909907

totalYears, validYears = 0, 0
for key, value in master.iteritems():
    totalYears += len(value[1])
    for y in value[1]:
        validYears += 1 if y != 0 else 0

print "Total Years: ", totalYears #9477100
print "Valid years: ", validYears #9426004, in short this is the count of 0s in the URL_id_file.txt


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


f = open("/usr/local/apache2/htdocs/ISI/wtnic/automation_Fall2017/summer/scalability/normalized_naics_code.txt", "r")
firmIds = ast.literal_eval(f.read().strip())
f.close()

print len(firmIds) # 729216

new_firmIds = {}
repeats = set()
for url, value in firmIds.iteritems():
    new_url = normalize_url(url)
    if new_url not in new_firmIds:
        new_firmIds[new_url] = value
    else:
        repeats.add(url)

print len(new_firmIds) # 726891

#repeats were written to a file - repeat_urls_removing_scheme.txt

ywc = open("year_wise_private_companies_count.txt", "w")
directory = "yearwise_companies_list/"

for year, companies in yearWiseCompanies.iteritems():
    f = open(directory + str(year) + ".txt", "w")
    ycount = 0
    for c in companies:
        if c in new_firmIds:
            ycount += 1
            f.write(c + "\n")
    f.close()
    ywc.write(str(year) + "\t" + str(ycount) + "\n")
ywc.close()

f = open("master_id_to_firm_id.txt", "w")
f.write("company\tmaster_id\tfirm_id\n")
master_firm_id = {}
for key, value in master.iteritems():
    if key in new_firmIds:
        company = key
        master_id = value[0]
        firm_id = new_firmIds[key][0]
        f.write(company + "\t" + str(master_id) + "\t" + str(firm_id) + "\n")
        master_firm_id[key] = (master_id, firm_id)
f.close()

f = open("masterId_firmId_mapping.txt", "w")
f.write(repr(master_firm_id))
f.close()

f = open("new_normalized_naics_code.txt", "w")
f.write(repr(new_firmIds))
f.close()
