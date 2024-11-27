import openpyxl
import psycopg2
from psycopg2 import sql

wb = openpyxl.load_workbook('campbell_library_inventory.xlsx')
sheet = wb['Sheet1']

for row in sheet.iter_rows(min_row=2, max_col=7):
  title = row[0]
  author = row[1]
  illustrator = row[2]
  bookType = row[3]
  category = row[4]
  subject = row[5]
  weh = row[6]