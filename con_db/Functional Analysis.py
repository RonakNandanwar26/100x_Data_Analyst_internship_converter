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


L1_count_df = L1_L2_overall_df.groupby(['L1'],as_index=False).count()

L1_count_df = L1_count_df.drop(L1_count_df.iloc[:,[2]],axis=1)

L1_count_df.columns = ['L1','COUNT of L1']
L1_count_df.sort_values(by='COUNT of L1',ascending=False,ignore_index=True,inplace=True)



L2_count_df = L1_L2_overall_df.groupby(['L1','L2'],as_index=False).count()
L2_count_df.columns = ['L1','L2','COUNT of L2']

L2_count_df = L2_count_df.sort_values(by=['L1','COUNT of L2'],ascending=[True,False],ignore_index=True)

pivot_table_df = pd.concat([L1_count_df,L2_count_df],axis=1)




L1_count_list = L1_count_df.values.tolist() # gives list of list of software name and its number of occurance
# pp.pprint(L1_count_list)

L1_graph_lol = []
others_lol = []


# if length of count list is greater or equal to 20 then top 9 softwares are added as it is with their 
# count and other software are added in 'others' and total count of 'others' is summed up and added as
# 'others' count
# if length of countlist is less then 20 then top half of them which contains more no of occurence
# are added as it is with thier count and  remaining half is added to 'others'
if len(L1_count_list)>=20:           
    for i in range(len(L1_count_list)):
        if i<9:
            L1_graph_lol.append(L1_count_list[i])
        else:
            others_lol.append(L1_count_list[i])
else:
    for i in range(int(len(L1_count_list)/2)):
        L1_graph_lol.append(L1_count_list[i])
    for j in range(int(len(L1_count_list)/2),len(L1_count_list)+1):
        others_lol.append(L1_count_list[j])


others = ['other']
sum = 0
for i in range(len(others_lol)):
    sum += others_lol[i][1]
others.append(sum)

L1_graph_lol.append(others)

L1_graph_df = pd.DataFrame(L1_graph_lol,columns=['Software Name','COUNT'])


L1_graph_df.to_sql(name='Functional Analysis',con=engine,index=False,if_exists='replace')

# <-------------------------------Functional Analysis Completed------------------------------------->


# select top 4 software name which has highest no. of occurence
top_4_soft = []  
for i in range(4):
    top_4_soft.append(L1_count_list[i][0])

L2_count_list = L2_count_df.values.tolist()  # coverts L2_count_df into list of list

# for first software which has highest no. of occurence
# 


first_soft = []
for i in range(len(L2_count_list)):
    if top_4_soft[0] == L2_count_list[i][0]:
        first_soft.append(L2_count_list[i])

# it will give list of list of L1, L2 and occurence of L2. we need only L2 and its occurence

for i in range(len(first_soft)):
    del first_soft[i][0]

# it will remove L1 from first soft list of list

first_soft_graph = []   #software names which are included in graph
first_soft_others = []   # software which are not in first_soft graph are added in this list

# if length of first soft is greater or equal to 20 then top 9 softwares are added as it is with their 
# count and other software are added in first_soft_others and total count of first_soft_others
# is summed up and added as 'others' count
# if length of first_soft is less then 20 then top half of them which contains more no of occurence
# are added as it is with thier count and  remaining half is added to 'first_soft_others'
if len(first_soft)>=20:           
    for i in range(len(first_soft)):
        if i<9:
            first_soft_graph.append(first_soft[i])
        else:
            first_soft_others.append(first_soft[i])
else:
    for i in range(int(len(first_soft)/2)):
        first_soft_graph.append(first_soft[i])
    for j in range(int(len(first_soft)/2),len(first_soft)):
        first_soft_others.append(first_soft[j])


# counting the occurence of software available in first_soft_others
others = ['other']
add = 0
for i in range(len(first_soft_others)):
    add = add + first_soft_others[i][1]
others.append(add)

first_soft_graph.append(others)


first_soft_df = pd.DataFrame(first_soft,columns=['L1','COUNT'])
first_soft_graph_df = pd.DataFrame(first_soft_graph,columns=['L1','COUNT'])

first_soft_final_df = pd.concat([first_soft_df,first_soft_graph_df],axis=1)


first_soft_final_df.to_sql(name=top_4_soft[0],con=engine,index=False,if_exists='replace')

# <---------------------------Highest no. of L2 occurence graph details completed-------------------------------->



second_soft = []
for i in range(len(L2_count_list)):
    if top_4_soft[1] == L2_count_list[i][0]:
        second_soft.append(L2_count_list[i])

# it will give list of list of L1, L2 and occurence of L2. we need only L2 and its occurence

for i in range(len(second_soft)):
    del second_soft[i][0]

# it will remove L1 from first soft list of list

second_soft_graph = []   #software names which are included in graph
second_soft_others = []   # software which are not in second_soft_graph are added in this list


if len(second_soft)>=20:           
    for i in range(len(second_soft)):
        if i<9:
            second_soft_graph.append(second_soft[i])
        else:
            second_soft_others.append(second_soft[i])
else:
    for i in range(int(len(second_soft)/2)):
        second_soft_graph.append(second_soft[i])
    for j in range(int(len(second_soft)/2),len(second_soft)):
        second_soft_others.append(second_soft[j])



others = ['other']
add = 0
for i in range(len(second_soft_others)):
    add = add + second_soft_others[i][1]
others.append(add)

second_soft_graph.append(others)


second_soft_df = pd.DataFrame(second_soft,columns=['L1','COUNT'])   
second_soft_graph_df = pd.DataFrame(second_soft_graph,columns=['L1','COUNT'])

second_soft_final_df = pd.concat([second_soft_df,second_soft_graph_df],axis=1)


second_soft_final_df.to_sql(name=top_4_soft[1],con=engine,index=False,if_exists='replace')


# <--------------------------Second Highest no. of L2 occurence graph details completed-------------------------------->


third_soft = []
for i in range(len(L2_count_list)):
    if top_4_soft[2] == L2_count_list[i][0]:
        third_soft.append(L2_count_list[i])

# it will give list of list of L1, L2 and occurence of L2. we need only L2 and its occurence

for i in range(len(third_soft)):
    del third_soft[i][0]

# it will remove L1 from first soft list of list

third_soft_graph = []   #software names which are included in graph
third_soft_others = []   # software which are not in second_soft_graph are added in this list

if len(third_soft)>=20:           
    for i in range(len(third_soft)):
        if i<9:
            third_soft_graph.append(third_soft[i])
        else:
            third_soft_others.append(third_soft[i])
else:
    for i in range(int(len(third_soft)/2)):
        third_soft_graph.append(third_soft[i])
    for j in range(int(len(third_soft)/2),len(third_soft)):
        third_soft_others.append(third_soft[j])



others = ['other']
add = 0
for i in range(len(third_soft_others)):
    add = add + third_soft_others[i][1]
others.append(add)

third_soft_graph.append(others)


third_soft_df = pd.DataFrame(third_soft,columns=['L1','COUNT'])   
third_soft_graph_df = pd.DataFrame(third_soft_graph,columns=['L1','COUNT'])

third_soft_final_df = pd.concat([third_soft_df,third_soft_graph_df],axis=1)


third_soft_final_df.to_sql(name=top_4_soft[2],con=engine,index=False,if_exists='replace')


# <--------------------------Third Highest no. of L2 occurence graph details completed-------------------------------->


fourth_soft = []
for i in range(len(L2_count_list)):
    if top_4_soft[3] == L2_count_list[i][0]:
        fourth_soft.append(L2_count_list[i])

# it will give list of list of L1, L2 and occurence of L2. we need only L2 and its occurence

for i in range(len(fourth_soft)):
    del fourth_soft[i][0]

# it will remove L1 from first soft list of list

fourth_soft_graph = []   #software names which are included in graph
fourth_soft_others = []   # software which are not in second_soft_graph are added in this list

if len(fourth_soft)>=20:           
    for i in range(len(fourth_soft)):
        if i<9:
            fourth_soft_graph.append(fourth_soft[i])
        else:
            fourth_soft_others.append(fourth_soft[i])
else:
    for i in range(int(len(fourth_soft)/2)):
        fourth_soft_graph.append(fourth_soft[i])
    for j in range(int(len(fourth_soft)/2),len(fourth_soft)):
        fourth_soft_others.append(fourth_soft[j])



others = ['other']
add = 0
for i in range(len(fourth_soft_others)):
    add = add + fourth_soft_others[i][1]
others.append(add)

fourth_soft_graph.append(others)


fourth_soft_df = pd.DataFrame(fourth_soft,columns=['L1','COUNT'])   
fourth_soft_graph_df = pd.DataFrame(fourth_soft_graph,columns=['L1','COUNT'])

fourth_soft_final_df = pd.concat([fourth_soft_df,fourth_soft_graph_df],axis=1)

fourth_soft_final_df.to_sql(name=top_4_soft[3],con=engine,index=False,if_exists='replace')
# <--------------------------Fourth Highest no. of L2 occurence graph details completed-------------------------------->
