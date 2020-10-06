import sqlalchemy
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import gspread

engine = sqlalchemy.create_engine('mysql+pymysql://root:admin@localhost:3306/test')


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

raw_df = pd.DataFrame(data)

raw_df.to_sql(name='raw_data',con=engine,index=False,if_exists='replace')
