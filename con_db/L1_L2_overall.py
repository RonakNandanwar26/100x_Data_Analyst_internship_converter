import sqlalchemy
import pandas as pd
import pymysql
import pprint

pp = pprint.PrettyPrinter()

engine = sqlalchemy.create_engine('mysql+pymysql://root:admin@localhost:3306/test')

raw_df = pd.read_sql('raw_data',engine)



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


integration_lol = Integration_df.to_numpy().tolist()  # this will give list of list of row wise integrations

s2c = [list(i) for i in universal_df.itertuples(index=False)] # converted softwarename list available in to universal sheet into list of lists


cat = []   # category list
cat_lol = []   # category list of list row wise

# in this category according to integration name is given in output list cat which stands for category
# this category list will converted into list of list and appended it cat_lol which stands for 
# category list of list
for i in range(len(integration_lol)):
    cat.clear()
    for j in range(len(integration_lol[i])):
        for k in range(len(s2c)):
            if integration_lol[i][j] == s2c[k][0]:
                cat.append(s2c[k][1])
                
    cat = [cat]
    cat_lol += cat

category_df = pd.DataFrame(cat_lol)   # cat_lol is converted to dataframe

L1_L2_cat_list = category_df.values.tolist()

L1_L2_cat_list = [[str(L1_L2_cat_list[i][j]) for j in range(len(L1_L2_cat_list[i])) if str(L1_L2_cat_list[i][j]) != 'None']for i in range(len(L1_L2_cat_list))]

L1_L2_cat_lol = [[[str(L1_L2_cat_list[i][j])]for j in range(len(L1_L2_cat_list[i]))]for i in range(len(L1_L2_cat_list))]

for i in range(len(L1_L2_cat_lol)):
    for j in range(len(L1_L2_cat_lol[i])):
        for k in range(len(L1_L2_cat_lol[i][j])):
            L1_L2_cat_lol[i][j].append(L1_L2_cat_lol[i][j][k].split(' - ',1))
        for l in range(len(L1_L2_cat_lol[i][j][1])):
            L1_L2_cat_lol[i][j].append(L1_L2_cat_lol[i][j][1][l])


new_L1_L2_cat = []
for i in range(len(L1_L2_cat_lol)):
    for j in range(len(L1_L2_cat_lol[i])):
        new_L1_L2_cat.append(L1_L2_cat_lol[i][j])

L1_L2_overall_df = pd.DataFrame(new_L1_L2_cat)

L1_L2_overall_df = L1_L2_overall_df.drop(1,axis=1)

L1_L2_overall_df.columns = ['L1-L2 overall','L1','L2']




L1_L2_overall_df.to_sql(name='L1-L2 Overall data',con=engine,index=False,if_exists='replace')