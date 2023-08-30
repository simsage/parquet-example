#!/usr/bin/env python3

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

# pii
col_ccard = 0
col_person = 0
col_nin = 0
col_ssn = 0
col_email = 0

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
sim_dictionary = dict()
identical_dictionary = dict()
url_lookup = dict()

counter = 0
with open(input_file, 'rt') as reader:
    for l in  csv.reader(reader, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
        
        counter += 1
        if counter == 1:
            # process header
            header_counter = 0
            for item in l:
                if item == "credit_card_count":
                    col_ccard = header_counter
                elif item == "nin_count":
                    col_nin = header_counter
                elif item == "ssn_count":
                    col_ssn = header_counter
                elif item == "person_count":
                    col_person = header_counter
                elif item == "email_count":
                    col_email = header_counter
                header_counter += 1
            continue

        item_id = l[0] # the id of this item
        item_url = l[1]
        url_lookup[item_id] = item_url  # id -> url
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

        # for PII collection
        ccard = 0
        person = 0
        nin = 0
        ssn = 0
        email = 0

        if col_ccard > 0:
            ccard = int(l[col_ccard])
        if col_person > 0:
            person = int(l[col_person])
        if col_nin > 0:
            nin = int(l[col_nin])
        if col_ssn > 0:
            ssn = int(l[col_ssn])
        if col_email > 0:
            email = int(l[col_email])

        acls = l[col_acls].split(",")
        content_hash = l[col_conthash]
        similar = l[col_similar].split(",")

        # gather information by document extension type
        if len(extn) == 0:
            extn = 'unknown'
        if len(sub_type) == 0:
            sub_type = "unknown"
        if extn in type_dictionary:
            existing = type_dictionary[extn]
        else:
            existing = {"sub_type_set": {}, "byte_size": 0, "oldest": 0, "newest": 0}
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
            if "names of people" in pii_dictionary:
                pii_dictionary["names of people"] += person
            else:
                pii_dictionary["names of people"] = person
        if nin > 0:
            if "national insurance numbers" in pii_dictionary:
                pii_dictionary["national insurance numbers"] += nin
            else:
                pii_dictionary["national insurance numbers"] = nin
        if ccard > 0:
            if "credit cards" in pii_dictionary:
                pii_dictionary["credit cards"] += ccard
            else:
                pii_dictionary["credit cards"] = ccard
        if ssn > 0:
            if "social security numbers" in pii_dictionary:
                pii_dictionary["social security numbers"] += ssn
            else:
                pii_dictionary["social security numbers"] = ssn
        if email > 0:
            if "email addresses" in pii_dictionary:
                pii_dictionary["email addresses"] += email
            else:
                pii_dictionary["email addresses"] = email

        # gather security data (acl distributions)
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

        # gather similar documents
        if len(similar) > 0 and len(similar[0]) > 0:
            for item in similar:
                parts = item.split("@")
                similar_list = [item_id]
                if len(parts) == 2:
                    similar_id = parts[0]
                    percentage = parts[1]
                    # take the smallest of the two ids
                    first_id = item_id
                    second_id = similar_id
                    if second_id < first_id:
                        first_id = similar_id
                        second_id = item_id
                    key = "{}:{}".format(first_id, second_id)
                    sim_dictionary[key] = percentage

        if len(content_hash) > 0:
            if content_hash not in identical_dictionary:
                identical_dictionary[content_hash] = []
            identical_dictionary[content_hash].append(item_id)

# set up the identical items inside the sim dictionary
for item in identical_dictionary:
    values = identical_dictionary[item]
    if len(values) > 1:
        for i in values:
            for j in values:
                if i == j:
                    continue
                first_id = i
                second_id = j
                if second_id < first_id:
                    first_id = j
                    second_id = i
                key = "{}:{}".format(first_id, second_id)
                sim_dictionary[key] = '1.0'

# output the data so it can be processed

# a report of the file-extensions to size / newest / oldest and counts
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

with open(output_prefix + 'similarity_report.csv', 'wt') as writer:
    for sim in sim_dictionary:
        ids = sim.split(":")
        if len(ids) == 2:
            percentage = sim_dictionary[sim]
            url_1 = url_lookup[ids[0]]
            url_2 = url_lookup[ids[1]]
            writer.write("{},{},{}\n".format(url_1, url_2, str(percentage)))
