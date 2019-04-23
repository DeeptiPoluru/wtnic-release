import urltools
import ast


def normalize_url(company):
    web = company.replace("_", "/")
    if web.startswith("http://") or web.startswith("https://"):
        parse = urltools.parse(web)
    else:
        web = "http://" + web
        parse = urltools.parse(web)
    url = parse.domain + "." + parse.tld
    return url


f = open("/usr/local/apache2/htdocs/ISI/wtnic/automation_Fall2017/summer/scalability/master/new_normalized_naics_code.txt", "r")
naics_code = ast.literal_eval(f.read().strip())
f.close()

print "Initial naics_code size: ", len(naics_code)

firm_id = 0
for key, value in naics_code.iteritems():
    if firm_id < value[0]:
        firm_id = value[0]
firm_id += 1

print "next firm id: ", firm_id

f = open("../masterId_firmId_mapping.txt", "r")
master_firm_id = ast.literal_eval(f.read().strip())
f.close()

f = open('../URL_id_file.txt', "r")
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
        master[company] = [id, {year}]

    if year in yearWiseCompanies:
        yearWiseCompanies[year].add(company)
    else:
        yearWiseCompanies[year] = {company}

f.close()

print "Total lines: ", lineCount #9477100
print "Size of master: ", len(master) #909907

for company, value in master.iteritems():
    master_id, years = value[0], value[1]
    company = normalize_url(company)
    if company not in naics_code:
        naics_code[company] = (firm_id, "", "")
        master_firm_id[company] = (master_id, firm_id)
        firm_id += 1

print "New firms size: ", len(naics_code)

f = open("company_firmid_dict.txt", "w")
f.write(repr(naics_code))
f.close()

f = open("masterId_firmId_mapping_dict.txt", "w")
f.write(repr(master_firm_id))
f.close()

f = open("master_id_to_firm_id.txt", "w")
f.write("company\tmaster_id\tfirm_id\n")
for company, value in master_firm_id.iteritems():
    f.write(company + "\t" + str(value[0]) + "\t" + str(value[1]) + "\n")
f.close()

indexed_companies = set(naics_code.keys())
master_companies = set(master_firm_id.keys())
diff = master_companies - indexed_companies
print "Total unindexed companies from master file: ", len(diff)

'''
output: 04/15/2019

Initial naics_code size:  726891
next firm id:  729218
Total lines:  9477100
Size of master:  909907
New firms size:  914043
Total unindexed companies from master file:  0
'''