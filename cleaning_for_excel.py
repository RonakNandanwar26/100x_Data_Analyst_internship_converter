import pandas as pd
import numpy as np
import pprint
pd.options.mode.chained_assignment = None


pp = pprint.PrettyPrinter()

# converting sheet with integrations to dataframe named AB_df
AB_df = pd.read_csv('Neelima_Raw_sheet.csv')

# converting required sheet in df
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
new_cols = other_column + Integration_column
AB_df.columns = new_cols
pp.pprint(AB_df)


# coverting only integration column of AB_df to dataframe
Integration_df = AB_df[Integration_column]
# pp.pprint(Integration_df)


# making a list of integration available in the AB_df
Integration_list = []
for i in range(len(Integration_df.index)):
    Integration_list += Integration_df.values[i].tolist()
# pp.pprint(Integration_list)


# making a new list for distinct integrations available in AB_df
unique_integration_set = set(Integration_list)
unique_integration_list = list(unique_integration_set)
# pp.pprint(unique_integration_list)
# print(len(unique_integration_list))


# removiung nan and zzz from unique_integration_list
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
# pp.pprint(len(sorted(unique_universal_integration_list)))


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
# print(len(universal_integration_dictionary))


# step 1
# In this step elements of unique_integration_list are searched in universal_integration_list keys
# and if element of unique_integration_list found in universal_integration_dictionary
# then this element is changed to its standard value using universal_integration_dictionary
# values and updated in the AB_df
not_found_integration = []
for i in range(len(unique_integration_list)):
    for key,value in universal_integration_dictionary.items():
        if str(key).casefold() == str(unique_integration_list[i]).casefold():
            Integration_df.replace(to_replace=unique_integration_list[i], value=value,inplace=True)
            break
    else:
        not_found_integration.append(unique_integration_list[i])

# print('Integration not found in step1 '+str(len(not_found_integration)))


# step 2 
# In this step space between integrations are removed and then convert all integration in lower case
# then this integration names are searched in the dictionary by converting all dictionary key and value
# in lower and replace space between integration name. one dictionary is made for the integrations which are found 
# which were not found in previous step named dct. After this search the not found integration in previous step and 
# search it in dct, and change it with value available in dct.
dct = {}
found_integration = []
for i in range(len(not_found_integration)):
    for key,value in universal_integration_dictionary.items():
        if str(not_found_integration[i]).casefold().replace(' ','') == str(key).casefold().replace(' ',''):
            dct[not_found_integration[i]] = value
            found_integration.append(not_found_integration[i])
            break
    

        # else:
  #             invalid_integration.append(not_found_integration[i])

# pp.pprint(dct)
# print('found integration in step 2'+ str(len(found_integration)))


for i in range(len(found_integration)):
    for key,value in dct.items():
        if found_integration[i].casefold().replace(' ','') == key.casefold().replace(' ',''):
            Integration_df.replace(to_replace=found_integration[i],value=value, inplace=True)
            break




# print('no. of integrations not found '+str(len(invalid_integration)))

other_column_df = AB_df[other_column]
final_df = pd.concat([other_column_df,Integration_df],axis=1)
final_df.to_csv(input('Enter a name you want to store your cleaned file')+str('.csv'), index=False)
# pp.pprint('Names of Integrations which are not found: \n'+str(not_found_integration))



 
sn2an_found = {}
for i in range(len(unique_integration_list)):
    for key,value in universal_integration_dictionary.items():
        if str(unique_integration_list[i]).casefold().replace(' ','') == str(key).casefold().replace(' ',''):
            sn2an_found[unique_integration_list[i]] = value
            break


print('sn2an found: '+str(len(sn2an_found)))


sn2an_found_list = []
for key in sn2an_found:
    sn2an_found_list.append(key)


sn2an_notfound = list(set(unique_integration_list)-set(sn2an_found_list))
print('sn2an_notfound: '+ str(len(sn2an_notfound)))


sn2an_notfound_dict = {}
for key in sn2an_notfound:
    sn2an_notfound_dict[key] = np.nan



# pp.pprint(sn2an)
# print(len(sn2an))
sn2an_df = pd.DataFrame(list(sn2an_found.items()),columns = ['Software Name','Actual Name'])
sn2an_notfound_df = pd.DataFrame(list(sn2an_notfound_dict.items()),columns = ['Software Name','Actual Name'])


sn2an_df.to_csv('found_integration.csv', index=False)
sn2an_notfound_df.to_csv('not_found_integration.csv', index=False)
