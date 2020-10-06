import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import numpy as np
import pprint
from df2gspread import df2gspread as d2g
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from itertools import repeat
pd.options.mode.chained_assignment = None


# authentication with google spreadsheet
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
json = input('Note: json file and this python file must be in same folder or path.\nEnter the name of json file: ')
creds = ServiceAccountCredentials.from_json_keyfile_name(json, scope)
client = gspread.authorize(creds)

# opening first sheet from google sheet
google_spreadsheet = input('Enter the name of your google spreadsheet: ')
sheet = client.open(google_spreadsheet)

raw_sheet = client.open(google_spreadsheet).sheet1
data = raw_sheet.get_all_records()
pp = pprint.PrettyPrinter()


raw_df = pd.DataFrame(data)


# coverting required sheet in df
converter_df = pd.read_csv('Updated Converter_7th Aug - Sheet1.csv')
universal_df = pd.read_csv('Updated Cush Sheet as on 17 August - Updated Cush.csv')


# creating column names for the integration
Integration_column = []
no_of_column_not_containing_integration = int(input('Enter a number of column which is not containing Integrations: '))
for i in range(1,len(raw_df.columns)-(no_of_column_not_containing_integration-1)):
    Integration_column.append('Integration '+str(i))
# pp.pprint(Integration_column)


other_column = []
for i in range(no_of_column_not_containing_integration):
    col = input('Enter name of column '+ str(i+1)+' which are not containing integrations: ')
    other_column.append(col)
pp.pprint(other_column)

new_cols = other_column + Integration_column
raw_df.columns = new_cols

# coverting only integration column of raw_df to dataframe
Integration_df = raw_df[Integration_column]
# pp.pprint(Integration_df)

dictionary = {'/':' ','\s+':' ',r'^\s+':'',r'\s+$':''}   # replace '/' with " ", '\s+' with " ", r'^\s+'->left strip,r'\s+$'->Right strip
Integration_df.replace(dictionary,regex=True,inplace=True)

# replacing null cells with space
Integration_df.replace('','zzz',inplace=True)


# making a list of integration available in the raw_df
Integration_list = []
for i in range(len(Integration_df.index)):
    Integration_list += Integration_df.values[i].tolist()



# making a new list for distinct integrations available in AB_df
unique_integration_set = set(Integration_list)
unique_integration_list = list(unique_integration_set)
# pp.pprint(unique_integration_list)
# print(len(unique_integration_list))

unique_integration_list = [i for i in unique_integration_list if str(i)!= 'nan']
print(len(unique_integration_list))


# list Software name available in converter df
Software_Name = converter_df['Software Name'].tolist()
# pp.pprint(Software_Name)

# list Actual spelling of Software name available in converter df
Actual_Spelling = converter_df['Actual Spelling'].tolist()
# pp.pprint(Actual_Spelling)


# making dictionary to map Software Name to their Actual Spelling
converter_dictionary = {}
for key in Software_Name:
    for value in Actual_Spelling:
        converter_dictionary[key] = value
        Actual_Spelling.remove(value)
        break

# pp.pprint(converter_dictionary)


# making list of Integration available in universal sheet
universal_integration_list = universal_df['Software Name'].tolist()
unique_universal_integration_list = list(set(universal_integration_list))
# pp.pprint(len(sorted(universal_integration_list)))


# making dictionary for unique universal integration list
universal_integration_dictionary = {}
for key in unique_universal_integration_list:
    for value in unique_universal_integration_list:
        if key == value:
            universal_integration_dictionary[key] = value
            break
# pp.pprint(universal_integration_dictionary)


# merging universal integration dictionary and converter dictionary
universal_integration_dictionary.update(converter_dictionary)
# print('universal integration dictionary: '+str(len(universal_integration_dictionary)))


found_integration_dct = {}
# step 1
# In this step elements of unique_integration_list are searched in universal_integration_list keys
# and if element of unique_integration_list found in universal_integration_dictionary
# then this element is changed to its standard value using universal_integration_dictionary
# values and updated in the AB_df
found_integration_list_in_step1 = []
found_integration_dct_in_step1 = {}
not_found_integration_in_step1 = []
for i in range(len(unique_integration_list)):
    for key,value in universal_integration_dictionary.items():
        if str(key).casefold() == str(unique_integration_list[i]).casefold():
            Integration_df.replace(to_replace=unique_integration_list[i], value=value,inplace=True)
            found_integration_list_in_step1.append(unique_integration_list[i])
            found_integration_dct_in_step1[key] = value
            break
    else:
        not_found_integration_in_step1.append(unique_integration_list[i])
found_integration_dct.update(found_integration_dct_in_step1)

# print('Integration found in step1: ' + str(len(found_integration_list_in_step1)))
# print('Integration not found in step1: '+str(len(not_found_integration_in_step1)))
# print('found_integration_dct length after step 1: '+ str(len(found_integration_dct)))



# step 2 
# In this step space between integrations are removed and then convert all integration in lower case
# then this integration names are searched in the dictionary by converting all dictionary key and value
# in lower and replace space between integration name. one dictionary is made for the integrations which are found 
# which were not found in previous step named dct. After this search the not found integration in previous step and 
# search it in dct, and change it with value available in dct.
found_integration_dct_in_step2 = {}
found_integration_in_step2 = []

for i in range(len(not_found_integration_in_step1)):
    for key,value in universal_integration_dictionary.items():
        if str(not_found_integration_in_step1[i]).casefold().replace(' ','') == str(key).casefold().replace(' ',''):
            found_integration_dct_in_step2[not_found_integration_in_step1[i]] = value
            found_integration_in_step2.append(not_found_integration_in_step1[i])
            break

not_found_integration_in_step2 = list(set(not_found_integration_in_step1)-set(found_integration_in_step2))
# print('not found integration after step 2: '+str(len(not_found_integration_in_step2)))

found_integration_dct.update(found_integration_dct_in_step2)
# print('found integration dct after step 2: '+str(len(found_integration_dct)))
# del found_integration_dct['zzz']       

# pp.pprint(dct)
# print('found integration in step 2 '+ str(len(found_integration_in_step2)))



for i in range(len(found_integration_in_step2)):
    for key,value in found_integration_dct_in_step2.items():
        if found_integration_in_step2[i].casefold().replace(' ','') == key.casefold().replace(' ',''):
            Integration_df.replace(to_replace=found_integration_in_step2[i],value=value, inplace=True)
            break


not_found_integration = not_found_integration_in_step2
for i in range(len(not_found_integration)):
    Integration_df.replace(not_found_integration[i],'zzz',inplace=True)

other_column_df = raw_df[other_column]
final_df = pd.concat([other_column_df,Integration_df],axis=1)

sn2an_df = pd.DataFrame(sorted(list(found_integration_dct.items())), columns=['Software Name','Actual Name'])
sn2an_notfound_df = pd.DataFrame(not_found_integration, columns=['Software Name'])



# Software to its Category mapping


# s2c = software to category list of lists
s2c = [list(i) for i in universal_df.itertuples(index=False)] # converted softwarename list available in to universal sheet into list of lists


# a new list made for found integration using software count dct
found_integration_list = []
for i in found_integration_dct.values():
    if i not in found_integration_list:
        found_integration_list.append(str(i).lower())

found_integration_list = [i for i in found_integration_list if str(i)!= 'zzz']  # remove 'zzz' if there any
# print('found_integration_list length: '+str(len(found_integration_list)))




# found integration list will convert into list of list
found_integration_lol = [[i] for i in found_integration_list] # found_integration_list is converted into list of list
pp.pprint(sorted(found_integration_lol))

# print(len(found_integration_lol))



# this code will map software name to its all available category using the Universal sheet(The Cush sheet)
for i in range(len(found_integration_lol)):
    for j in range(len(s2c)):
        if str(found_integration_lol[i][0]).casefold().replace(' ','') == str(s2c[j][0]).casefold().replace(' ',''):
            found_integration_lol[i].append(s2c[j][1])

            
pp.pprint(found_integration_lol)

s2c_df = pd.DataFrame(sorted(found_integration_lol),columns=None)   # convert mapped list(found_integration_lol) into dataframe

cat_cols = []
for i in range(len(s2c_df.columns)-1):
    cat_cols.append('Category '+str(i+1))

mapping_cols = ["software"] + cat_cols

s2c_df.columns = mapping_cols

s2c_df.fillna('zzz',inplace=True)
s2c = s2c_df.values.tolist()
s2c_list = [[ j for j in i if j != 'zzz' ] for i in s2c] 
s2c_list = [[str(s2c_list[i][j]).split(' - ',1) for j in range(len(s2c_list[i]))]for i in range(len(s2c_list))]

for i in range(len(s2c_list)):
    for j in range(len(s2c_list[i])):
        if len(s2c_list[i][j])>1:
            s2c_list[i][j].remove(s2c_list[i][j][0])


software = s2c_df[["software"]]        
software_lol = software.values.tolist()



Essential = ['CRM Software', 'Marketing Automation Software',' Sales Acceleration Software',' Advertiser Campaign Management Software',' Conversion Rate Optimization Software',' Data Integration Software',' Social Media Marketing Software',' Marketing Analytics Software',' Conversational Marketing Software',' Web Content Management Software',' API Marketplace Software',' Accounting & Finance Software',' Mobile Marketing Software',' Demand Generation Software',' Email Marketing Software',' Sales Intelligence Software']
Recommended = ['Social Networks Software','Internal Communications Software',' Productivity Bots Software',' Calendar Software',' Project, Portfolio & Program Management Software',' Email Software',' Marketplace Apps',' Email Deliverability Software',' ERP Systems',' Application Development Software',' Identity Management Software',' Office Suites Software',' Talent Management Software',' Contact Center Software',' Event Management Software',' Cloud Content Collaboration Software',' HelpDesk Software',' Quote Management Software',' Discrete ERP Software',' Distribution ERP Software',' Email Client Software',' Customer Self-Service Software']


new_s2c_list = []
temp_list = []
for i in range(len(s2c_list)):
    for j in s2c_list[i]:
        for k in j:
            if k in temp_list:
                continue
            else:
                temp_list.append(k)
    new_s2c_list.append(list(temp_list))
    temp_list.clear()


new_soft = software_lol


dct = {'Essential':0,'Recommended':0,'other':0,'count':0}
for i in range(len(new_s2c_list)):
    for j in range(1,len(new_s2c_list[i])):
        if new_s2c_list[i][j] in Essential:
            dct['Essential'] = dct.get('Essential',0)+1
        elif new_s2c_list[i][j] in Recommended:
            dct['Recommended'] = dct.get('Recommended',0)+1
        else:
            dct['other'] = dct.get('other',0)+1
    denominator = dct['Essential'] + dct['Recommended'] + dct['other']
    try:
        dct['count'] = ((0.5/denominator)*(dct['Essential']))+((0.35/denominator)*(dct['Recommended']))+((0.15/denominator)*(dct['other']))
    except:
        dct['count'] = (0.5*(dct['Essential']))+(0.35*(dct['Recommended']))+(0.15*(dct['other']))
    for k in range(len(software_lol)):
        if str(new_s2c_list[i][0]).casefold().replace(' ','') == str(software_lol[k][0]).casefold().replace(' ',''):
            software_lol[k].append(list(dct.values()))
            dct = dict.fromkeys(dct,0)


temp = []
for i in range(len(software_lol)):
    for j in range(1,len(software_lol[i])):
        for k in range(len(software_lol[i][j])):
            new_soft[i].append(software_lol[i][j][k])

for i in new_soft:
    i.pop(1)                

Category_count_df = pd.DataFrame(new_soft)
Category_count_df = Category_count_df.iloc[:,0:5]
Category_count_df.columns = ['Integration name','Essential','Recommended','other','Category count']

Category_count_df.sort_values(by='Category count',ascending=False,ignore_index=True,inplace=True)


try:
    sheet.add_worksheet(rows=1000,cols=100,title='Category_count')
except:
    category_count_sheet = client.open(google_spreadsheet).worksheet('Category_count')

category_count_sheet = client.open(google_spreadsheet).worksheet('Category_count')

set_with_dataframe(category_count_sheet,Category_count_df)    



# <------------------category count sheet complete------------------------>


soft = raw_df[['Title']]
Int_df = raw_df[Integration_column]
soft_int_df = pd.concat([soft,Int_df],axis=1)
soft_int_list = soft_int_df.values.tolist()
soft_int = [[j for j in i if str(j)!='nan' and str(j)!='zzz']for i in soft_int_list]
software = soft.values.tolist()


dct = {'Essential':0,'Recommended':0,'other':0,'count':0}
for i in range(len(software)):
    for j in range(len(soft_int)):
        if str(software[i][0]).casefold().replace(' ','') == str(soft_int[j][0]).casefold().replace(' ',''):
            for k in range(1,len(soft_int[j])):
                for l in range(len(new_soft)):
                    if str(soft_int[j][k]).casefold().replace(' ','') == str(new_soft[l][0]).casefold().replace(' ',''):
                        if new_soft[l][1] >= 1:
                            dct['Essential'] = dct.get('Essential',0)+1
                            
                        elif new_soft[l][1] == 0 and new_soft[l][2] >=1:
                            dct['Recommended'] = dct.get('Recommended',0)+1
                            
                        elif new_soft[l][1] == 0 and new_soft[l][2] == 0  and new_soft[l][3]>=1:
                            dct['other'] = dct.get('other',0)+1
                        
    dct['count'] = 0.5*dct['Essential'] + 0.35*dct['Recommended'] + 0.15*dct['other']
    software[i].append(list(dct.values()))
    dct = dict.fromkeys(dct,0)



for i in range(len(software)):
    for j in range(1,len(software[i])):
        for k in range(len(software[i][j])):
            software[i].append(software[i][j][k])

for i in software:
    i.pop(1)

Int_count_df = pd.DataFrame(software)
Int_count_df.columns = ['Software name','Essential','Recommended','other','Integration count']

Int_count_df.sort_values(by='Integration count',ascending=False,ignore_index=True,inplace=True)                



try:
    sheet.add_worksheet(rows=1000,cols=100,title='Integration_count')
except:
    category_count_sheet = client.open(google_spreadsheet).worksheet('Integration_count')

Integration_count_sheet = client.open(google_spreadsheet).worksheet('Integration_count')

set_with_dataframe(Integration_count_sheet,Int_count_df)    






















