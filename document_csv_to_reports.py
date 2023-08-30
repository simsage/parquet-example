import csv
import sys

# convert a document export CSV to three content reports for a high level overview of the data

col_type = 6
col_extn = 7
col_created = 8
col_lastmod = 9
col_conthash = 10
col_size = 11
col_acls = 12
col_similar = 13
col_ccard = 17
col_person = 21
col_nin = 22

if len(sys.argv) != 2:
    print("convert a document csv file to a series of four reports")
    print("takes one parameter: /path/to/document-export.csv")
    exit(1)

input_file = sys.argv[1]
output_prefix = input_file
parts = input_file.split(".")
if len(parts) == 2:
    output_prefix = parts[0]
output_prefix += "-"

type_dictionary = dict()
pii_dictionary = dict()
sec_dictionary = dict()

counter = 0
with open('sjic-file-store.csv', 'rt') as reader:
    for l in  csv.reader(reader, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
        
        counter += 1
        if counter == 1:
            continue

        extn = l[col_extn]
        sub_type = l[col_type]
        c_created = l[col_created]
        c_lastmod = l[col_lastmod]
        created = 0
        if len(c_created) > 0:
            created = int(c_created)
        last_mod = 0
        if len(c_lastmod) > 0:
            last_mod = int(c_lastmod)
        byte_size = int(l[col_size])
        ccard = int(l[col_ccard])
        person = int(l[col_person])
        nin = int(l[col_nin])
        acls = l[col_acls].split(",")
        content_hash = l[col_conthash]
        similar = l[col_similar].split(",")

        # gather information by document extension type
        if len(extn) == 0:
            extn = 'unknow'
        if extn in type_dictionary:
            existing = type_dictionary[extn]
        else:
            existing = {"sub_type_set": {}, "byte_size": 0, "oldest": 0, "newest": 0}
        if len(sub_type) > 0:
            if sub_type in existing["sub_type_set"]:
                existing["sub_type_set"][sub_type] += 1
            else:
                existing["sub_type_set"][sub_type] = 1
        if created > 0 and last_mod > 0:
            oldest = created
            newest = last_mod
            if existing["oldest"] == 0 or existing["oldest"] > oldest:
                existing["oldest"] = oldest
            if existing["newest"] == 0 or existing["newest"] < newest:
                existing["newest"] = newest
        existing["byte_size"] += byte_size
        type_dictionary[extn] = existing

        # gather pii
        if person > 0:
            if "person" in pii_dictionary:
                pii_dictionary["person"] += person
            else:
                pii_dictionary["person"] = person
        if nin > 0:
            if "nin" in pii_dictionary:
                pii_dictionary["nin"] += nin
            else:
                pii_dictionary["nin"] = nin
        if ccard > 0:
            if "ccard" in pii_dictionary:
                pii_dictionary["ccard"] += ccard
            else:
                pii_dictionary["ccard"] = ccard

        for hv in acls:
            parts = hv.split(":")
            if len(parts) == 2:
                access = parts[1].upper()
                who = parts[0].lower()
                if access not in sec_dictionary:
                    sec_dictionary[access] = {}
                if who in sec_dictionary[access]:
                    sec_dictionary[access][who] += 1
                else:
                    sec_dictionary[access][who] = 1


# output the data so it can be processed
with open(output_prefix + 'type_report_1.csv', 'wt') as writer:
    for t in type_dictionary:
        sub_type = type_dictionary[t]
        byte_size = sub_type["byte_size"]
        oldest = sub_type["oldest"]
        newest = sub_type["newest"]
        sub_type_set = sub_type["sub_type_set"]
        total = 0
        total_byte_size = 0
        for sub in sub_type_set:
            count = sub_type_set[sub]
            total += count
        writer.write("{},{},{},{},{}\n".format(t, byte_size, oldest, newest, str(total)))

with open(output_prefix + 'type_report_2.csv', 'wt') as writer:
    for t in type_dictionary:
        sub_type = type_dictionary[t]
        sub_type_set = sub_type["sub_type_set"]
        for sub in sub_type_set:
            writer.write("{},\"{}\",{}\n".format(t, sub, str(count)))

with open(output_prefix + 'pii_report.csv', 'wt') as writer:
    for pii in pii_dictionary:
        count = pii_dictionary[pii]
        writer.write("{},{}\n".format(pii, str(count)))

with open(output_prefix + 'sec_report.csv', 'wt') as writer:
    for sec in sec_dictionary:
        sec_set = sec_dictionary[sec]
        for who in sec_set:
            count = sec_set[who]
            writer.write("{},{},{}\n".format(who, str(count), sec))

