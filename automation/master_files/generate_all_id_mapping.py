import ast

f = open("company_firmid_dict.txt", "r")
firmid = ast.literal_eval(f.read())
f.close()
print "Firm_id size: ", len(firmid) # 914043


result = open("all_id_mapping.csv", "w")
result.write("company\tfirm_id\tnaics\tobtained_from\n")
gv, count = 0, 0
for key, value in firmid.iteritems():
    firm_id, naics, obtained = value[0], value[1], value[2]
    result.write(key + "\t" + str(firm_id) + "\t" + str(naics) + "\t" + str(obtained) + "\n")
    count += 1

result.close()
print "Total count ", count # 914043
