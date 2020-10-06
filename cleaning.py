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


# opening a second sheet in google sheet 
# must be secondsheet in google sheet  
sheet1 = client.open(google_spreadsheet).get_worksheet(0)
data = sheet1.get_all_records()
# sheet.add_worksheet(rows = 1000,cols = 100,title='found_integration')
sheet2 = client.open(google_spreadsheet).get_worksheet(1)
# sheet.add_worksheet(rows = 1000,cols = 100,title='not_found_ntegrations')
sheet3 = client.open(google_spreadsheet).get_worksheet(2)
# sheet.add_worksheet(rows = 1000,cols = 100,title='category_mapping')
sheet4 = client.open(google_spreadsheet).get_worksheet(3)
pp = pprint.PrettyPrinter()

# converting sheet with integrations to dataframe named AB_df
AB_df = pd.DataFrame(data)
# pp.pprint(AB_df)


# coverting required sheet in df
converter_df = pd.read_csv('Updated Converter_7th Aug - Sheet1.csv')
universal_df = pd.read_csv('Updated Cush Sheet as on 17 August - Updated Cush.csv')


# creating column names for the integration
Integration_column = []
no_of_column_not_containing_integration = int(input('Enter a number of column which is not containing Integrations: '))
for i in range(1,len(AB_df.columns)-(no_of_column_not_containing_integration-1)):
    Integration_column.append('Integration '+str(i))
# pp.pprint(Integration_column)


other_column = []
for i in range(no_of_column_not_containing_integration):
    col = input('Enter name of column '+ str(i+1)+' which are not containing integrations: ')
    other_column.append(col)
pp.pprint(other_column)

# updating column name to AB_df
# other_column = ['Title', 'Review URL', 'G2 Score', 'Description', 'Reviews', 'Comments']
# new_cols = other_column + Integration_column
# AB_df.columns = new_cols
# pp.pprint(AB_df)


# coverting only integration column of AB_df to dataframe
Integration_df = AB_df[Integration_column]
# pp.pprint(Integration_df)

dictionary = {'/':' ','\s+':' ',r'^\s+':'',r'\s+$':''}   # replace '/' with " ", '\s+' with " ", r'^\s+'->left strip,r'\s+$'->Right strip
Integration_df.replace(dictionary,regex=True,inplace=True)

# replacing null cells with space
Integration_df.replace('','zzz',inplace=True)


# making a list of integration available in the AB_df
Integration_list = []
for i in range(len(Integration_df.index)):
    Integration_list += Integration_df.values[i].tolist()
# pp.pprint(Integration_list)


# making a new list for distinct integrations available in AB_df
unique_integration_set = set(Integration_list)
old_unique_integration_list = list(unique_integration_set)
# pp.pprint(unique_integration_list)
# print(len(unique_integration_list))
integration_list_lower = []
for i in range(len(old_unique_integration_list)):
    integration_list_lower.append(str(old_unique_integration_list[i]).lower().replace(' ',''))

unique_integration_list_lower = list(set(integration_list_lower))
# print(len(unique_integration_list_lower))

unique_integration_dict = {}
for key in unique_integration_list_lower:
    for value in old_unique_integration_list:
        if key == str(value).lower().replace(' ',''):
            unique_integration_dict[key] = value 
            del key
            break
# print(len(unique_integration_dict))

unique_integration_list = []

for value in unique_integration_dict.values():
    unique_integration_list.append(str(value))

# pp.pprint(unique_integration_list)
# print(len(unique_integration_list))


# removiung nan from unique_integration_list
unique_integration_list = [i for i in unique_integration_list if str(i)!= 'nan']
unique_integration_list = [i for i in unique_integration_list if str(i)!= 'zzz']

print('unique integration list: '+ str(len(unique_integration_list)))

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
pp.pprint(len(sorted(universal_integration_list)))


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
print('universal integration dictionary: '+str(len(universal_integration_dictionary)))




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

print('Integration found in step1: ' + str(len(found_integration_list_in_step1)))
print('Integration not found in step1: '+str(len(not_found_integration_in_step1)))
print('found_integration_dct length after step 1: '+ str(len(found_integration_dct)))





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
print('not found integration after step 2: '+str(len(not_found_integration_in_step2)))

found_integration_dct.update(found_integration_dct_in_step2)
print('found integration dct after step 2: '+str(len(found_integration_dct)))
# del found_integration_dct['zzz']       

# pp.pprint(dct)
print('found integration in step 2 '+ str(len(found_integration_in_step2)))



for i in range(len(found_integration_in_step2)):
    for key,value in found_integration_dct_in_step2.items():
        if found_integration_in_step2[i].casefold().replace(' ','') == key.casefold().replace(' ',''):
            Integration_df.replace(to_replace=found_integration_in_step2[i],value=value, inplace=True)
            break




other_column_df = AB_df[other_column]
final_df = pd.concat([other_column_df,Integration_df],axis=1)
final_df.to_csv('cleaned.csv',index=False)

not_found_integration = not_found_integration_in_step2
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
print('found_integration_list length: '+str(len(found_integration_list)))




# found integration list will convert into list of list
found_integration_lol = [[i] for i in found_integration_list] # found_integration_list is converted into list of list
pp.pprint(sorted(found_integration_lol))
print(len(found_integration_lol))



# this code will map software name to its all available category using the Universal sheet(The Cush sheet)
for i in range(len(found_integration_lol)):
    for j in range(len(s2c)):
        if str(found_integration_lol[i][0]).casefold().replace(' ','') == str(s2c[j][0]).casefold().replace(' ',''):
            found_integration_lol[i].append(s2c[j][1])

            
pp.pprint(found_integration_lol)

s2c_df = pd.DataFrame(sorted(found_integration_lol),columns=None)   # convert mapped list(found_integration_lol) into dataframe
# s2c_df.to_csv('s2c.csv')


# s2c_df = pd.DataFrame(s2c_df, columns=None)



# updating data to google sheet
set_with_dataframe(sheet1,final_df)
set_with_dataframe(sheet2,sn2an_df)
set_with_dataframe(sheet3,sn2an_notfound_df)
set_with_dataframe(sheet4,s2c_df)




# <------------------------Cleaning and Mapping Part is done------------------------------------->
 

# # sheet.add_worksheet(rows=500,cols=500,title='Final Integrals')
# sheet5 = client.open(google_spreadsheet).get_worksheet(4)

# # sname_df is a dataframe of software name available in raw sheet
# sname_df = AB_df['Title']


# final_integral_df = pd.concat([sname_df,Integration_df],axis=1) # this dataframe will contain only sofware name and its integrations
# final_integral_df.replace('zzz','',inplace=True)  # it replaces zzz with null
# final_integral_df.replace('',np.NaN,inplace=True) # it replaces null cells with NaN



# Integration_df.replace('zzz',np.NaN,inplace=True)  # replace 'zzz' with null
# integration_lol = Integration_df.to_numpy().tolist()  # this will give list of list of row wise integrations
# pp.pprint(integration_lol)


# cat = []   # category list
# cat_lol = []   # category list of list row wise



# # in this category according to integration name is given in output list cat which stands for category
# # this category list will converted into list of list and appended it cat_lol which stands for 
# # category list of list
# for i in range(len(integration_lol)):
#     cat.clear()
#     for j in range(len(integration_lol[i])):
#         for k in range(len(s2c)):
#             if integration_lol[i][j] == s2c[k][0]:
#                 cat.append(s2c[k][1])
                
#     cat = [cat]
#     cat_lol += cat

# category_df = pd.DataFrame(cat_lol)   # cat_lol is converted to dataframe


# # category_column list contain new column names for category_df
# category_column = []
# for i in range(1,len(category_df.columns)+1):
#     category_column.append('Category '+str(i))

# category_df.columns = category_column
# new_final_integral_df = pd.concat([final_integral_df,category_df],axis=1) #final_integral_df and category_df is now joined

# new_final_integral_df['Integration_count'] = final_integral_df[Integration_column].count(axis=1)  # counts the total number of integration available in Integration in each row

# new_final_integral_df['category_count'] = new_final_integral_df[category_column].count(axis=1) #counts the total number of categories available in according to integration in each row 

# # gives number of unique integration categories available in each row
# unique_category_list = []
# for i in range(len(cat_lol)):
#     unique_category_list.append(len(list(set(cat_lol[i]))))

# unique_category_df = pd.DataFrame(unique_category_list)  # for conviniency unique_category_list is converted to dataframe so we can join it to final_integral_df
# new_final_integral_df['unique_category_count'] = unique_category_df # new column added to show unique category available in each row


# # moving integration_count, category_count, unique_category_count beside the software name 
# df = new_final_integral_df['Integration_count']
# new_final_integral_df.drop(labels=['Integration_count'],axis=1, inplace=True)
# new_final_integral_df.insert(1,'Integration_count',df)

# df = new_final_integral_df['unique_category_count']
# new_final_integral_df.drop(labels=['unique_category_count'],axis=1,inplace=True)
# new_final_integral_df.insert(2,'unique_category_count',df)

# df = new_final_integral_df['category_count']
# new_final_integral_df.drop(labels='category_count',axis=1,inplace=True)
# new_final_integral_df.insert(3,'category_count',df)


# new_final_integral_df.sort_values(by='Integration_count',ascending=False,inplace=True) # sorting data according to the descending order of Integration count


# set_with_dataframe(sheet5,new_final_integral_df)

# # <---------unique integration count-category count and unique category count completed ----------------->





# # sheet.add_worksheet(rows = 3000,cols=1000, title='L1-L2 Overall data') 
# sheet6 = client.open(google_spreadsheet).get_worksheet(5)



# L1_L2_cat_list = category_df.values.tolist()

# L1_L2_cat_list = [[str(L1_L2_cat_list[i][j]) for j in range(len(L1_L2_cat_list[i])) if str(L1_L2_cat_list[i][j]) != 'None']for i in range(len(L1_L2_cat_list))]

# L1_L2_cat_lol = [[[str(L1_L2_cat_list[i][j])]for j in range(len(L1_L2_cat_list[i]))]for i in range(len(L1_L2_cat_list))]

# for i in range(len(L1_L2_cat_lol)):
#     for j in range(len(L1_L2_cat_lol[i])):
#         for k in range(len(L1_L2_cat_lol[i][j])):
#             L1_L2_cat_lol[i][j].append(L1_L2_cat_lol[i][j][k].split(' - ',1))
#         for l in range(len(L1_L2_cat_lol[i][j][1])):
#             L1_L2_cat_lol[i][j].append(L1_L2_cat_lol[i][j][1][l])


# new_L1_L2_cat = []
# for i in range(len(L1_L2_cat_lol)):
#     for j in range(len(L1_L2_cat_lol[i])):
#         new_L1_L2_cat.append(L1_L2_cat_lol[i][j])

# L1_L2_overall_df = pd.DataFrame(new_L1_L2_cat)

# L1_L2_overall_df = L1_L2_overall_df.drop(1,axis=1)

# L1_L2_overall_df.columns = ['L1-L2 overall','L1','L2']

# set_with_dataframe(sheet6,L1_L2_overall_df)


# # <------------L1-L2 categories are separated and updated in L1-L2 overall sheet------------>

# # sheet.add_worksheet(rows = 1000,cols=100, title='Pivot Table')
# sheet7 = client.open(google_spreadsheet).get_worksheet(6)

# # counting the occurance of every integration of category L1

# L1_count_df = L1_L2_split_df.groupby(['L1'],as_index=False).count()
# L1_count_cols = ['L1','COUNTA_OF_L1']
# L1_count_df.columns = L1_count_cols
# L1_count_df = L1_count_df.sort_values(by='COUNTA_OF_L1',ascending=False,ignore_index=True)


# # counting the occurance of L2 based on L1 
# L2_count_df = L1_L2_split_df.groupby(['L1','L2'],as_index=False).size()
# L2_count_cols = ['L1','L2','COUNTA_OF_L2']
# L2_count_df.columns = L2_count_cols
# L2_count_df = L2_count_df.sort_values(['L1','COUNTA_OF_L2'],ascending=[True,False],ignore_index=True)

# Pivot_Table_df = pd.concat([L1_count_df,L2_count_df],axis=1)

# set_with_dataframe(sheet7,Pivot_Table_df)

# #<----------------------------------Pivot table completed--------------------------------------------> 

# # sheet.add_worksheet(rows=500,cols=100,title='Functional Analysis')
# sheet8 = client.open(google_spreadsheet).worksheet('Functional Analysis')


# L1_count_list = L1_count_df.values.tolist() # gives list of list of software name and its number of occurance
# # pp.pprint(L1_count_list)

# L1_graph_lol = []
# others_lol = []


# # if length of count list is greater or equal to 20 then top 9 softwares are added as it is with their 
# # count and other software are added in 'others' and total count of 'others' is summed up and added as
# # 'others' count
# # if length of countlist is less then 20 then top half of them which contains more no of occurence
# # are added as it is with thier count and  remaining half is added to 'others'
# if len(L1_count_list)>=20:           
#     for i in range(len(L1_count_list)):
#         if i<9:
#             L1_graph_lol.append(L1_count_list[i])
#         else:
#             others_lol.append(L1_count_list[i])
# else:
#     for i in range(int(len(L1_count_list)/2)):
#         L1_graph_lol.append(L1_count_list[i])
#     for j in range(int(len(L1_count_list)/2),len(L1_count_list)+1):
#         others_lol.append(L1_count_list[j])


# others = ['other']
# sum = 0
# for i in range(len(others_lol)):
#     sum += others_lol[i][1]
# others.append(sum)

# L1_graph_lol.append(others)

# L1_graph_df = pd.DataFrame(L1_graph_lol,columns=['Software Name','COUNT'])

# set_with_dataframe(sheet8,L1_graph_df)


# # <-------------------------------Functional Analysis Completed------------------------------------->


# # select top 4 software name which has highest no. of occurence
# top_4_soft = []  
# for i in range(4):
#     top_4_soft.append(L1_count_list[i][0])

# L2_count_list = L2_count_df.values.tolist()  # coverts L2_count_df into list of list

# # for first software which has highest no. of occurence
# # 
# try:
#     sheet.add_worksheet(rows=500,cols=100,title=top_4_soft[0])
# except:
#     sheet9 = client.open(google_spreadsheet).worksheet(top_4_soft[0])

# sheet9 = client.open(google_spreadsheet).worksheet(top_4_soft[0])



# first_soft = []
# for i in range(len(L2_count_list)):
#     if top_4_soft[0] == L2_count_list[i][0]:
#         first_soft.append(L2_count_list[i])

# # it will give list of list of L1, L2 and occurence of L2. we need only L2 and its occurence

# for i in range(len(first_soft)):
#     del first_soft[i][0]

# # it will remove L1 from first soft list of list

# first_soft_graph = []   #software names which are included in graph
# first_soft_others = []   # software which are not in first_soft graph are added in this list

# # if length of first soft is greater or equal to 20 then top 9 softwares are added as it is with their 
# # count and other software are added in first_soft_others and total count of first_soft_others
# # is summed up and added as 'others' count
# # if length of first_soft is less then 20 then top half of them which contains more no of occurence
# # are added as it is with thier count and  remaining half is added to 'first_soft_others'
# if len(first_soft)>=20:           
#     for i in range(len(first_soft)):
#         if i<9:
#             first_soft_graph.append(first_soft[i])
#         else:
#             first_soft_others.append(first_soft[i])
# else:
#     for i in range(int(len(first_soft)/2)):
#         first_soft_graph.append(first_soft[i])
#     for j in range(int(len(first_soft)/2),len(first_soft)):
#         first_soft_others.append(first_soft[j])


# # counting the occurence of software available in first_soft_others
# others = ['other']
# add = 0
# for i in range(len(first_soft_others)):
#     add = add + first_soft_others[i][1]
# others.append(add)

# first_soft_graph.append(others)


# first_soft_df = pd.DataFrame(first_soft,columns=['L1','COUNT'])
# first_soft_graph_df = pd.DataFrame(first_soft_graph,columns=['L1','COUNT'])

# first_soft_final_df = pd.concat([first_soft_df,first_soft_graph_df],axis=1)

# set_with_dataframe(sheet9,first_soft_final_df)


# # <---------------------------Highest no. of L2 occurence graph details completed-------------------------------->
# try:
#     sheet.add_worksheet(rows=500,cols=100,title=top_4_soft[1])
# except:
#     sheet10 = client.open(google_spreadsheet).worksheet(top_4_soft[1])

# sheet10 = client.open(google_spreadsheet).worksheet(top_4_soft[1])


# second_soft = []
# for i in range(len(L2_count_list)):
#     if top_4_soft[1] == L2_count_list[i][0]:
#         second_soft.append(L2_count_list[i])

# # it will give list of list of L1, L2 and occurence of L2. we need only L2 and its occurence

# for i in range(len(second_soft)):
#     del second_soft[i][0]

# # it will remove L1 from first soft list of list

# second_soft_graph = []   #software names which are included in graph
# second_soft_others = []   # software which are not in second_soft_graph are added in this list


# if len(second_soft)>=20:           
#     for i in range(len(second_soft)):
#         if i<9:
#             second_soft_graph.append(second_soft[i])
#         else:
#             second_soft_others.append(second_soft[i])
# else:
#     for i in range(int(len(second_soft)/2)):
#         second_soft_graph.append(second_soft[i])
#     for j in range(int(len(second_soft)/2),len(second_soft)):
#         second_soft_others.append(second_soft[j])



# others = ['other']
# add = 0
# for i in range(len(second_soft_others)):
#     add = add + second_soft_others[i][1]
# others.append(add)

# second_soft_graph.append(others)


# second_soft_df = pd.DataFrame(second_soft,columns=['L1','COUNT'])   
# second_soft_graph_df = pd.DataFrame(second_soft_graph,columns=['L1','COUNT'])

# second_soft_final_df = pd.concat([second_soft_df,second_soft_graph_df],axis=1)

# set_with_dataframe(sheet10,second_soft_final_df)

# # <--------------------------Second Highest no. of L2 occurence graph details completed-------------------------------->
# try:
#     sheet.add_worksheet(rows=500,cols=100,title=top_4_soft[2])
# except:
#     sheet11 = client.open(google_spreadsheet).worksheet(top_4_soft[2])

# sheet11 = client.open(google_spreadsheet).worksheet(top_4_soft[2])


# third_soft = []
# for i in range(len(L2_count_list)):
#     if top_4_soft[2] == L2_count_list[i][0]:
#         third_soft.append(L2_count_list[i])

# # it will give list of list of L1, L2 and occurence of L2. we need only L2 and its occurence

# for i in range(len(third_soft)):
#     del third_soft[i][0]

# # it will remove L1 from first soft list of list

# third_soft_graph = []   #software names which are included in graph
# third_soft_others = []   # software which are not in second_soft_graph are added in this list

# if len(third_soft)>=20:           
#     for i in range(len(third_soft)):
#         if i<9:
#             third_soft_graph.append(third_soft[i])
#         else:
#             third_soft_others.append(third_soft[i])
# else:
#     for i in range(int(len(third_soft)/2)):
#         third_soft_graph.append(third_soft[i])
#     for j in range(int(len(third_soft)/2),len(third_soft)):
#         third_soft_others.append(third_soft[j])



# others = ['other']
# add = 0
# for i in range(len(third_soft_others)):
#     add = add + third_soft_others[i][1]
# others.append(add)

# third_soft_graph.append(others)


# third_soft_df = pd.DataFrame(third_soft,columns=['L1','COUNT'])   
# third_soft_graph_df = pd.DataFrame(third_soft_graph,columns=['L1','COUNT'])

# third_soft_final_df = pd.concat([third_soft_df,third_soft_graph_df],axis=1)

# set_with_dataframe(sheet11,third_soft_final_df)



# # <--------------------------Third Highest no. of L2 occurence graph details completed-------------------------------->
# try:
#     sheet.add_worksheet(rows=500,cols=100,title=top_4_soft[3])
# except: 
#     sheet12 = client.open(google_spreadsheet).worksheet(top_4_soft[3])

# sheet12 = client.open(google_spreadsheet).worksheet(top_4_soft[3])

# fourth_soft = []
# for i in range(len(L2_count_list)):
#     if top_4_soft[3] == L2_count_list[i][0]:
#         fourth_soft.append(L2_count_list[i])

# # it will give list of list of L1, L2 and occurence of L2. we need only L2 and its occurence

# for i in range(len(fourth_soft)):
#     del fourth_soft[i][0]

# # it will remove L1 from first soft list of list

# fourth_soft_graph = []   #software names which are included in graph
# fourth_soft_others = []   # software which are not in second_soft_graph are added in this list

# if len(fourth_soft)>=20:           
#     for i in range(len(fourth_soft)):
#         if i<9:
#             fourth_soft_graph.append(fourth_soft[i])
#         else:
#             fourth_soft_others.append(fourth_soft[i])
# else:
#     for i in range(int(len(fourth_soft)/2)):
#         fourth_soft_graph.append(fourth_soft[i])
#     for j in range(int(len(fourth_soft)/2),len(fourth_soft)):
#         fourth_soft_others.append(fourth_soft[j])



# others = ['other']
# add = 0
# for i in range(len(fourth_soft_others)):
#     add = add + fourth_soft_others[i][1]
# others.append(add)

# fourth_soft_graph.append(others)


# fourth_soft_df = pd.DataFrame(fourth_soft,columns=['L1','COUNT'])   
# fourth_soft_graph_df = pd.DataFrame(fourth_soft_graph,columns=['L1','COUNT'])

# fourth_soft_final_df = pd.concat([fourth_soft_df,fourth_soft_graph_df],axis=1)

# set_with_dataframe(sheet12,fourth_soft_final_df)

# # <--------------------------Fourth Highest no. of L2 occurence graph details completed-------------------------------->


# try:
#     sheet.add_worksheet(rows=500,cols=100,title='integrals max')
# except:
#     sheet13 = client.open(google_spreadsheet).worksheet('integrals max')

# sheet13 = client.open(google_spreadsheet).worksheet('integrals max')

# lol_Integration = Integration_df.values.tolist()   # give list of list available in integration column (row wise list)
# list_Integration = []   

# for i in range(len(lol_Integration)):
#     for j in range(len(lol_Integration[i])):
#         list_Integration.append(lol_Integration[i][j])

# # now list_Integration contains list of integration available in Integration column

# Integration_dct = {}

# for i in list_Integration:
#     Integration_dct[i] = Integration_dct.get(i,0)+1
# # now Integration dct will contain software name as key and its occurence in list as value

# if 'zzz' in Integration_dct:
#     del Integration_dct['zzz']   # removing zzz key from dictionary

# integrals_max_df = pd.DataFrame(Integration_dct.items(),columns=['Software','SUM of Integration Count'])

# integrals_max_df = integrals_max_df.sort_values(by='SUM of Integration Count', ascending=False,ignore_index=True)

# integrals_max_df.dropna(inplace=True)

# Most_Popular_Integrations = integrals_max_df.iloc[0:11]
# Most_Popular_Integrations[''] = 'Most Popular Integrations'

# integrals_max_df = pd.concat([integrals_max_df,Most_Popular_Integrations],axis=1)

# df = integrals_max_df['']
# integrals_max_df.drop('',axis=1,inplace=True)
# integrals_max_df.insert(3,'',df)


# set_with_dataframe(sheet13,integrals_max_df)

# # <----------------------------Integrals max sheet completed------------------------------->


# try:
#     sheet.add_worksheet(rows=500,cols=100,title='Top SW')
# except:
#     sheet14 = client.open(google_spreadsheet).worksheet('Top SW')

# sheet14 = client.open(google_spreadsheet).worksheet('Top SW')

# Top_sw_df = new_final_integral_df.iloc[:,[0,1]]
# Top_sw_df = Top_sw_df.sort_values(by='Integration_count',ascending=False,ignore_index=True)

# Best_Integrated_companies = Top_sw_df.iloc[0:11]
# Best_Integrated_companies[''] = 'Best Integrated companies'
# Top_sw_df = pd.concat([Top_sw_df,Best_Integrated_companies],axis=1)
# df = Top_sw_df['']
# Top_sw_df.drop('',axis=1,inplace=True)
# Top_sw_df.insert(3,'',df)

# set_with_dataframe(sheet14,Top_sw_df)


# # <---------------------------------Top SW completed----------------------------------------->


# try:
#     sheet.add_worksheet(rows=1000,cols=100,title='Functional Area Leader')
# except:
#     sheet15 = client.open(google_spreadsheet).worksheet('Functional Area Leader')

# sheet15 = client.open(google_spreadsheet).worksheet('Functional Area Leader')

# # Integration_list is availble which contains Integrations in Integration_df
# # in form of [[first row],[second row],...]
# # we need to make it one list of all integrations
# Integrations = Integration_list   # contains Integrations
# for i in range(len(Integration_list)):
#     for j in range(len(Integration_list[i])):
#         Integrations.append(Integration_list[i][j])

# # Integrations contains 'zzz', we need to remove it.
# while 'zzz' in Integrations:
#     Integrations.remove('zzz')

# # we need to assign category to each integrations, so making integrations list of list
# Integrations_lol = [[i] for i in Integrations]


# # we need software and category list
# s2c = [list(i) for i in universal_df.itertuples(index=False)]

# # now we need to map category(L1-L2) to particular softwares
# for i in range(len(Integrations_lol)):
#     for j in range(len(s2c)):
#         if str(Integrations_lol[i][0]).lower().replace(' ','') == str(s2c[j][0]).lower().replace(' ',''):
#             Integrations_lol[i].append(s2c[j][1])

# s2c_list = Integrations_lol # for meaningful naming convention

# # splitting L1 and L2 category, we only need L1 
# s2c_list = [[str(s2c_list[i][j]).split(' - ',1) for j in range(len(s2c_list[i]))]for i in range(len(s2c_list))]
# # this gives software name and its category splitted in L1 and L2 e.g [[[soft_name],[L1,L2]],[[soft_name][L1,L2]]]


# # removing L2 category
# for i in range(len(s2c_list)):
#     for j in range(len(s2c_list[i])):
#         if len(s2c_list[i][j])>1:
#             s2c_list[i][j].remove(s2c_list[i][j][1])



# # we need to count occurence of L1 category which contain 'nan' or 'Error' or 'Error -'
# L1_dct = {}
# for i in range(len(s2c_list)):
#     for j in range(1,len(s2c_list[i])):
#         L1_dct[s2c_list[i][j][0]] = L1_dct.get(s2c_list[i][j][0],0)+1

# L1_occurence_lot = [(k,v) for k,v in L1_dct.items()] # This will convert dictionary into list of tuples

# # we need to remove 'nan' or 'Error' or 'Error -'

# for i in L1_occurence_lot:
#     if i[0] == 'nan':
#         L1_occurence_lot.remove(i)
#     if i[0] == 'Error':
#         L1_occurence_lot.remove(i)
#     if i[0] == 'Error -':
#         L1_occurence_lot.remove(i)

# # convert list of tuples to dataframe
# L1_occurence_df = pd.DataFrame(L1_occurence_lot,columns=['L1','count'])
# L1_occurence_df.sort_values(by='count',ascending=False,ignore_index=True,inplace=True)
# # L1_occurence_df.drop('nan',inplace=True)

# # L1_lol contains L1 categories in the form of list of list
# L1_lol = []
# for i in L1_dct:
#     L1_lol.append([i])

# L1 = L1_lol  # future purpose

# # now we need to append categories to software
# for i in range(len(L1_lol)):
#     for j in range(len(s2c_list)):
#         for k in range(1,len(s2c_list[j])):
#             if L1_lol[i][0] == s2c_list[j][k][0]:
#                 L1_lol[i].append(s2c_list[j][0][0])


# # we need to count the particular software occurence in particular category
# List_software_occurence = []
# temp_dct = {}
# for i in range(len(L1_lol)):
#     for j in range(len(L1_lol[i])):
#         temp_dct[L1_lol[i][j]] = temp_dct.get(L1_lol[i][j],0)+1
#     List_software_occurence.append(dict(temp_dct))
#     temp_dct.clear()

# # sorting the software occurence in descending order
# int_occurence_lot = []
# for i in range(len(List_software_occurence)):
#     int_occurence_lot.append(sorted([(k,v) for k,v in List_software_occurence[i].items()],key = lambda x:x[1],reverse=True))

# # making pair of L1 cat with software of that category and occurence of software
# for i in range(len(L1)):
#     for j in range(len(int_occurence_lot)):
#         for k in range(len(int_occurence_lot[j])):
#             if L1[i][0] == int_occurence_lot[j][k][0]:
#                 L1[i].append(int_occurence_lot[j])


# for i in range(len(L1)):
#     while (len(L1[i]))!=2:
#         for k,j in enumerate(L1[i]):    
#             if k == 0:
#                 continue
#             elif k == len(L1[i])-1:
#                 continue
#             else:
#                 L1[i].remove(j)

# for i in range(len(L1)):
#     for j in range(1,len(L1[i])):
#         for k in L1[i][j]:
#             L1[i].append(k)
#         L1[i][1].clear()


# for i in L1:
#     if i[0] == 'Error' or i[0] =='Error -':
#         L1.remove(i)

# for i in range(len(L1)):
#     for j in L1[i]:
#         if j==[]:
#             L1[i].remove(j)


# L1_int_occurence_df = pd.DataFrame(L1)

# L1_int_occurence_df.drop(1,axis=1,inplace=True)
# L1_int_occurence_df.rename({0: 'Fun', 1: '(Int,count)'}, axis='columns',inplace=True)


# Functional_area_leader_df = pd.concat([L1_occurence_df,L1_int_occurence_df],axis=1)
# set_with_dataframe(sheet15,Functional_area_leader_df)























# # # remove L2 category from s2c_list
# # for i in range(len(s2c_list)):
# #     for j in range(len(s2c_list[i])):
# #         if len(s2c_list[i][j])>1:
# #             s2c_list[i][j].remove(s2c_list[i][j][1])

# # # creating lol of L1 category list
# # L1_lol = []
# # for i in L1_dct:
# #     L1_lol.append([i])

# # for i in L1_lol:
# #     if i[0] == 'nan':
# #         L1_lol.remove(i)
# #     if i[0] == 'Error':
# #         L1_lol.remove(i)
# #     if i[0] == 'Error -':
# #         L1_lol.remove(i)



# # L1 = L1_lol  #for future purpose
# # # adding integration beside category according to category
# # # e.g.  [['L1','all int of L1 category'],['L1','all int of L1 category']....]
# # for i in range(len(L1_lol)):
# #     for j in range(len(s2c_list)):
# #         for k in range(1,len(s2c_list[j])):
# #             if L1_lol[i][0] == s2c_list[j][k][0]:
# #                 L1_lol[i].append(s2c_list[j][0][0])


# # # it counts the occurence of each integration and make a list named List_software_occurence
# # List_software_occurence = []
# # temp_dct = {}
# # for i in range(len(L1_lol)):
# #     for j in range(len(L1_lol[i])):
# #         temp_dct[L1_lol[i][j]] = temp_dct.get(L1_lol[i][j],0)+1
# #     List_software_occurence.append(dict(temp_dct))
# #     temp_dct.clear()

# # # it converts dictionary of occurence of integrations convert it into tuple and 
# # # sort it in descending order
# # int_occurence_lot = []
# # for i in range(len(List_software_occurence)):
# #     int_occurence_lot.append(sorted([(k,v) for k,v in List_software_occurence[i].items()],key = lambda x:x[1],reverse=True))


# # # it joins integration corresponding to L1 category available in L1_list
# # # e.g.  [[L1,[(integration name corresponding to L1 category,occurence )]],...]
# # for i in range(len(L1)):
# #     for j in range(len(int_occurence_lot)):
# #         for k in range(len(int_occurence_lot[j])):
# #             if L1[i][0] == int_occurence_lot[j][k][0]:
# #                 L1[i].append(int_occurence_lot[j])

# # # pp.pprint(L1_list)  
# # for i in range(len(L1)):
# #     while (len(L1[i]))!=2:
# #         for k,j in enumerate(L1[i]):    
# #             if k == 0:
# #                 continue
# #             elif k == len(L1[i])-1:
# #                 continue
# #             else:
# #                 L1[i].remove(j)



# # # we can not have [[L1,[(integration name corresponding to L1 category,occurence )]],...] format
# # # so we append tuple (integration name corresponding to L1 category,occurence ) with L1
# # # like [['Development Software',[],[],('jira', 6),('airtable', 4),('zapier', 4)]    
# # for i in range(len(L1)):
# #     for j in range(1,len(L1[i])):
# #         for k in L1[i][j]:
# #             L1[i].append(k)
# #         L1[i][1].clear()


# # for i in L1:
# #     if i[0] == 'nan':
# #         L1.remove(i)
# #     if i[0] == 'Error':
# #         L1.remove(i)
# #     if i[0] == 'Error -':
# #         L1.remove(i)






