import bagit
import xml.etree.ElementTree as Xet
import pandas as pd
import sys, os, zipfile, shutil
from datetime import date

#Commandline input for running this file:
#py <filename> <zipped_file>
#Results in 3 different directories being created:
    #- <Unzipped directory>
    #- <Directory of new bagit containing original data>
    #- <Zipped version of the new directory>

namespaces = {'dc':'http://purl.org/dc/elements/1.1/'}

def get_string_format(root, type):

    type = root.findall("dc:" + type, namespaces)
    for t in type:
        print(t.text)
        return t.text.rstrip()

def get_Creator(filename):

    xmlparse = Xet.parse(filename)
    root = xmlparse.getroot()

    return get_string_format(root,"creator")

def CSV_Converter(filename, new_directory):
    columns = ["source_identifier","title","creator","description","publisher","date_created",
    "resource_type","language","rights_statement"]
    rows = []
    list = []

    xmlparse = Xet.parse(filename)
    root = xmlparse.getroot()

    rows.append({"source_identifier": "Work One",
                 "title": get_string_format(root, "title"),
                 "creator": get_string_format(root,"creator"),
                 "description": get_string_format(root,"description"),
                 "publisher": get_string_format(root,"publisher"),
                 "date_created": get_string_format(root,"date"),
                 "resource_type": get_string_format(root,"type"),
                 "language": get_string_format(root,"language"),
                 "rights_statement": get_string_format(root,"rights")})

    df = pd.DataFrame(rows, columns=columns)
    df.to_csv(new_directory+"/metadata.csv", encoding="utf-8", index=False)

#Unzip directory
zipped_file = sys.argv[-1]
with zipfile.ZipFile(zipped_file,'r') as zip_ref:
    zip_ref.extractall()

currentdate = date.today()
directory = os.path.splitext(zipped_file)[0]
new_directory = currentdate.strftime('%y-%m-%d') + "-" + directory


#Check if old bag is valid:
print(bagit.Bag(directory).is_valid())

#Make new directory for new bagit to be created and input all data information
shutil.copytree(directory+'/data', new_directory)

#Get XML file from metadata
for filename in os.listdir(directory + '/data/meta'):
    if not filename.endswith('.xml'): continue
    xmlfile = directory + '/data/meta/' + filename


#Create bag with contact name
creator = get_Creator(xmlfile)
#creator_split = creator.split(" ")
#creator_split.reverse()
#formatted_creator = " ".join(creator_split[1:]).replace(',', '')

bag = bagit.make_bag(new_directory, {'Contact-Name': creator})

#Create CSV file from XML file
CSV_Converter(xmlfile, new_directory)

print(bagit.Bag(new_directory).is_valid())
shutil.make_archive(new_directory, 'zip', new_directory)
