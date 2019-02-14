"""
Create Rendering Provider Specialty and Taxonomy SAS Formats

Managing Team: Data Governance
Analyst: Will D. Leone
Last Updated: February 5, 2019

Purpose:
  - Retrieve the latest CMS crosswalk for rendering provider specialty
    and taxonomy codes and descriptions.
  - Clean and export this data to SAS format tables on the SAS server.
  - Create a CSV copy of the crosswalk to accompany these datasets.

 Prerequisites:
  - In addition to installing Python/Anaconda on your computer,
    you will also need to install the requests and saspy modules using the
    'conda install requests' and 'conda install saspy' commands in Anaconda
    Prompt.
  - You will also need to configure saspy using the instructions given here:
      https://confluence.evolenthealth.com/display/AnalyticsActuarial
      /SASpy+configuration+tutorial

 Next Steps:
 The output SAS datasets and CSV file will be saved to the SOURCE file within
 the all-client format directory (/sasprod/dw/formats). Once QA is done, the
 old datasets will need to be saved to the OLD subdirectory and the new datasets
 will need to be pushed into the main directory.

"""

import requests
import csv
import os.path
import re
import saspy
import pandas as pd
import sys
from copy import deepcopy
from operator import itemgetter

# INPUT DATA PARAMETERS
landing = ('https://data.cms.gov/Medicare-Enrollment/'
           'CROSSWALK-MEDICARE-PROVIDER-SUPPLIER-to-HEALTHCARE/j75i-rw8y')
# Page with a download link to the needed CSV data
site_code = landing[len(landing) - landing[::-1].find('/'):]
# The download site for DATA.CMS.GOV uses the landing site's code
site = ('https://data.cms.gov/api/views/'
       + site_code
       + '/rows.csv?accessType=DOWNLOAD')
# This template can likely be used for any DATA.CMS.GOV file
ecode = 'utf-8-sig'
# utf-8 would work here, but utf-8-sig can also handle the byte order mark
# (BOM) characters commonly used in Excel UTF-8

# OUTPUT DATA PARAMETERS
sas = saspy.SASsession(cfgname='pdw_config')
sas_code = sas.submit("""
    LIBNAME fmt "/sasprod/dw/formats/source/staging";
    """)
grid = ("//grid/sasprod/dw/formats/source")
out_file = ("//grid/sasprod/dw/formats/source/references/"
            "cms_rendspec_taxrend_taxtype.csv")
out_rendspec = list()
out_taxrend = list()
out_taxtype = list()

# PROCESS WEB DATA IN MEMORY
with requests.Session() as my_session:
    raw_source = my_session.get(site)
    # Pull in the website's CSV data as a Requests object
    source = raw_source.content.decode(ecode)
    # Convert the website's CSV data into a decoded text string
    src_reader = csv.reader(source.splitlines(), delimiter=',')
    # Read in the the newly-created string line by line

    with open(out_file, 'w', newline = '') as foo:
        out_write = csv.writer(foo
                        , delimiter=','
                        , quotechar='\''
                        , quoting = csv.QUOTE_ALL)
        # Create a writer object for printing to a new output CSV file

        row_counter = 0
        header = str()
        prior_filled_row = list()

        for row in src_reader:
        # Process each row in the string created from the website data

            row_counter += 1
            if (row_counter == 1 and row != ['MEDICARE SPECIALTY CODE'
                        , 'MEDICARE PROVIDER/SUPPLIER TYPE DESCRIPTION'
                        , 'PROVIDER TAXONOMY CODE'
                        , 'PROVIDER TAXONOMY DESCRIPTION']):
                sys.exit('Warning: the web data structure has changed.')
            elif row_counter == 1:
                header = row
                out_write.writerow(header)
                continue

            # Don't add rows that are intended as comments in the CSV
            if row[0] and (row[0].strip()[0] == '['
                    or len(row) != 4):
                continue

            # Remove extra quotes and replace commas with a placeholder '.'
            # to be removed later on
            for i in range(len(row)):
                if ',' in row[i] and i == 1:
                    row[i] = row[i].strip('\'').replace(',', '.')
                elif ',' in row[i]:
                    row[i] = row[i].strip('\'').replace(',', '.')

            # Resolves issue where a single row contains >1 taxonomy code
            # and >1 taxonomy description.
            if (len(row[2]) > 10 and len(row[2]) % 10 == 0):
                tax_desc_init= row[3]
                if re.search('[a-z][A-Z]', tax_desc_init):
                    tax_desc_list = list()
                    while re.search('[a-z][A-Z]', tax_desc_init):
                        s = re.search('[a-z][A-Z]', tax_desc_init).start()
                        e = re.search('[a-z][A-Z]', tax_desc_init).end()
                        tax_desc_list.append(tax_desc_init[:s + 1])
                        tax_desc_init = tax_desc_init[e-1:]
                    tax_desc_list.append(tax_desc_init)
                    tax_desc_len = len(tax_desc_list)
                else:
                    tax_desc_list = []
                    tax_desc_len = 0
                    tax_desc = tax_desc_init
                for i in range(1, int(len(row[2])/10) + 1):
                    code = row[2][(10 * (i-1)):(10 * i)]
                    if (tax_desc_len >= i
                            and tax_desc_list[i-1]):
                        tax_desc = tax_desc_list[i-1]
                    new_row = [row[0]
                               , row[1]
                               , code
                               , tax_desc]

                    # Remove references (e.g., '[1]' and '[12]')
                    # Populate missing entries for taxrend
                    filled_row = deepcopy(new_row)
                    for index, entry in enumerate(new_row):
                        if not new_row[index]:
                            new_row[index] = 'N/A'
                            filled_row[index] = prior_filled_row[index]
                        else:
                            new_row[index] = re.sub('\[[0-9]+\]', '', entry)
                            filled_row[index] = new_row[index]

                    out_write.writerow(new_row)
                    out_rendspec.append(['rendspec'
                        , new_row[0].replace('.', ',').strip()
                        , new_row[1].replace('.', ',').strip()
                        , 'C'])
                    out_taxrend.append(['taxrend'
                        , filled_row[2].replace('.', ',').strip()
                        , filled_row[0].replace('.', ',').strip()
                        , 'C'])
                    out_taxtype.append(['taxtype'
                        , new_row[2].replace('.', ',').strip()
                        , new_row[3].replace('.', ',').strip()
                        , 'C'])
                    prior_filled_row = filled_row
            # For all other records, simply print to the CSV.
            else:
                # Remove references (e.g., '[1]' and '[12]')
                # Populate missing entries for taxrend
                filled_row = deepcopy(row)
                for index, entry in enumerate(row):
                    if not row[index]:
                        row[index] = 'N/A'
                        filled_row[index] = prior_filled_row[index]
                    else:
                        row[index] = re.sub('\[[0-9]+\]', '', entry)
                        filled_row[index] = row[index]

                out_write.writerow(row)
                out_rendspec.append(['rendspec'
                    , row[0].replace('.', ',').strip()
                    , row[1].replace('.', ',').strip()
                    , 'C'])
                out_taxrend.append(['taxrend'
                    , filled_row[2].replace('.', ',').strip()
                    , filled_row[0].replace('.', ',').strip()
                    , 'C'])
                if row[2] != 'N/A':
                    out_taxtype.append(['taxtype'
                        , row[2].replace('.', ',').strip()
                        , row[3].replace('.', ',').strip()
                        , 'C'])
                prior_filled_row = filled_row

# PREPARE THE FINAL PANDAS TABLE FOR OUTPUTING AS A SAS FORMAT TABLE
initial = {'rendspec': [out_rendspec, []]
           , 'taxrend': [out_taxrend, []]
           , 'taxtype': [out_taxtype, []]
          }

for name, ds_list in initial.items():
    ds = ds_list[0]
    ds_final = ds_list[1]
    ds.sort(key=itemgetter(1))
    # Remove duplicate records
    for index, row in enumerate(ds):
        start, label = row[1], row[2]
        if index > 0:
            prior_start = ds[index - 1][1]
        else:
            prior_start = []
        if (start != 'N/A' and label != 'N/A' and
                start != prior_start):
            ds_final.append(row)
        else:
            continue
    ds_final.sort(key=itemgetter(1,2))
    df = pd.DataFrame(data=ds_final
                      , columns=['fmtname', 'start', 'label', 'type'])
    sas_out = sas.df2sd(df, table=name, libref='fmt')

sas.disconnect()

# As needed, use the pandas chunksize options to process in batches:
# for chunk in pd.read_csv(filename, chunksize=N):
#   *do something with the chunk of N records*
