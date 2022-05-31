import requests
import json
import mysql.connector
import csv
import gspread
import sys
import os
import datetime
from dotenv import load_dotenv

now = datetime.datetime.now()

workpath = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(workpath, '.env')
load_dotenv(dotenv_path)


a_auth=(os.getenv('MS_Login'), os.getenv('MS_Pass')) 

cnx = mysql.connector.connect(user=os.getenv('SQL_user'), password=os.getenv('SQL_pass'),
                              host=os.getenv('SQL_host'),
                              database=os.getenv('SQL_db'))
cursor = cnx.cursor()
order=str(input())
try:
    query = """SELECT id_order.order_id, firstname, lastname, email, telephone, payment_city, payment_address_1, UPPER(pers_text), 
           id_order_product.name, id_order_product.product_id, GROUP_CONCAT(product_option_value_id), GROUP_CONCAT(value), quantity, FLOOR(price), id_order.total, payment_code

           FROM id_order LEFT JOIN id_order_product ON id_order_product.order_id=id_order.order_id 
           LEFT JOIN id_order_option ON id_order_option.order_product_id=id_order_product.order_product_id
           
           WHERE id_order.order_id = %s GROUP BY id_order_product.order_product_id ORDER BY id_order.order_id DESC"""

    cursor.execute(query, (order,))
    result = cursor.fetchall()
except:
    pass
cnx.close()

# connect to Google sheets and define func for adding text into a certain column
client = gspread.service_account(filename=workpath+"/helpid-65b47311f305.json")
sheet = client.open('Заказы на LazerMark')
sheet_instance = sheet.get_worksheet(2)

# define row/cell in google sheet for pasting personalization text
def print_text(plate, text):
   col_2_val = sheet_instance.col_values(1)
   print_row=len(col_2_val)+1

   while len(sheet_instance.get('{}{}'.format(plate, print_row)))>0:
      print_row+=1
   sheet_instance.update_acell('{}{}'.format(plate, print_row), text)

# prepare product data 
def products_dict(href, pr_type, price, qty):
   products_dict={
              "quantity": qty,
                "price": price,
                "discount": 0,
                "vat": 0,
              "assortment": {
                "meta": {
                  "href": href,
                  "type": pr_type,
                  "mediaType": "application/json"
                  }
               }
               }
   return products_dict

# finding product href code 
def get_product_href(p_type, code):
   return json.loads(requests.get('https://online.moysklad.ru/api/remap/1.2/entity/'+p_type+'?filter=externalCode='+code, auth=a_auth).text)['rows'][0]['meta']['href']

# type of plate specified in produts article
def get_plate(href):
   return str(json.loads(requests.get(href, auth=a_auth).text)['article'])

# define size of additional product which depends on main product size 
def add_prod_code(name, code):
   if "16-" in name:
      code = code+'.'+'17'
   if "-20" in name:
      code = code+'.'+'19'
   if "S" in name:
      code = code+'.'+'14'
   if "M" in name:
      code = code+'.'+'16'
   return code

products=[]
products_hrefs=[]
pers_text_list=[]

# unpack list of tuples from query
# if first pdoduct not bundle - reverse list
if len(result[0][7])<1:
   result = list(reversed(result))
   
for data_sql in result:
   order_id, firstname, lastname, email, telephone, payment_city, payment_address, pers_text, prod_name, prod_id, prod_info, value, qty, price, total, payment = data_sql
   
   price=int(price)*100
   
   if prod_info:
      prod_info=prod_info.replace(',','.')

   # by default first product in order is bundle, if not bundle - then type product 
   pr_type = 'bundle'
   code = prod_info
   if prod_info == None:
      pr_type='product'
      code = prod_id
   if '22mm' in prod_name or 'Garmin' in prod_name: 
      pr_type = 'bundle'
      code = prod_id

   # returning product code in crm 
   with open(workpath+'/codes.csv', 'r', encoding='UTF8') as file:
      reader = csv.reader(file)
      for row in reader:
         if code == row[1]:
            code = row[0]

   try:
      href = get_product_href(pr_type, str(code))
   except IndexError:
      try:
         for items in products:
            if '.' in items[1]:
               href = get_product_href(pr_type, add_prod_code(items[0], str(code)))
      except:
         if '.' in code:
            reverse_code=code.split('.')[1]+'.'+code.split('.')[0]
            href = get_product_href(pr_type, str(reverse_code))

   if pr_type == 'bundle':
      if value != None:
         products.append((value, prod_info))
      pers_text=str(pers_text.replace('<BR>', '\n'))
      pers_text_list.append((get_plate(href), pers_text.rstrip()))

   products_hrefs.append((href, pr_type, price, qty))


# join personalization text for order description
pers_text_all='\n\n'.join([i[1] for i in pers_text_list])

total_sum=int(total)*100

# check if customer exist to avoid duplicate, or create new 
customers = requests.get('https://online.moysklad.ru/api/remap/1.2/entity/counterparty?filter=phone~'+telephone, auth=a_auth)
data = json.loads(customers.text)['rows']
if data:
   n_href=data[0]['meta']['href']

# create customer
else:
   name = firstname+" "+lastname
   address=payment_city+" "+payment_address
   ms_name = {
    "name": name,
    "companyType": "individual",
    "actualAddress": address,
    "email": email,
    "phone": telephone
           }
   resp = requests.post('https://online.moysklad.ru/api/remap/1.2/entity/counterparty', json=ms_name, auth=a_auth)
   n = json.loads(resp.text) 
   n_href = (n["meta"]["href"])

# prepare order data 
ms_data = {
            "description": pers_text_all,
            "name": str(order_id),
            # "applicable": False,
            "organization": {
              "meta": {
                "href": "https://online.moysklad.ru/api/remap/1.2/entity/organization/0cad5148-5057-11eb-0a80-077800211373",
                "type": "organization",
                "mediaType": "application/json"
              }
            },
            "agent": {
              "meta": {
                "href": n_href,
                "type": "counterparty",
                "mediaType": "application/json"
              }
            },
            "store": {
              "meta": {
                "href": "https://online.moysklad.ru/api/remap/1.2/entity/store/0cae6367-5057-11eb-0a80-077800211375",
                "type": "store",
                "mediaType": "application/json"
              }
            },
            "state": {
            "meta": {
              "href": "https://online.moysklad.ru/api/remap/1.2/entity/demand/metadata/states/4ccfadcc-543d-11eb-0a80-050700001531",
              "metadataHref": "https://online.moysklad.ru/api/remap/1.2/entity/demand/metadata",
              "type": "state",
              "mediaType": "application/json"
                }
              },
            "positions": [
              # упаковка
               {
               "quantity": 1,
                 "price": 0,
                 "discount": 0,
                 "vat": 0,
               "assortment": {
                 "meta": {
                   "href": "https://online.moysklad.ru/api/remap/1.2/entity/product/a6007426-5413-11eb-0a80-04cb00511fe8",
                   "type": "product",
                   "mediaType": "application/json"
                   }
                }
                },
               {
              "quantity": len(pers_text_list),
                "price": 0,
                "discount": 0,
                "vat": 0,
              "assortment": {
                "meta": {
                  "href": "https://online.moysklad.ru/api/remap/1.2/entity/service/f7ae555e-5405-11eb-0a80-01b2004fefca",
                  "type": "service",
                  "mediaType": "application/json"
                  }
               }
               }]
            } 

for i in products_hrefs:
   ms_data["positions"].insert(products_hrefs.index(i), products_dict(href=i[0], pr_type=i[1], price=i[2], qty=i[3]))

order_resp = requests.post('https://online.moysklad.ru/api/remap/1.2/entity/demand', json=ms_data, auth=a_auth)

if order_resp.status_code != 200:
   resp=json.loads(order_resp.text)['errors'][0]['error']
   # if error - write it to logfile
   with open(workpath+'/logfile.log', 'a', encoding='UTF8') as file:
      file.write(str(now)[:19]+' Заказ '+str(order_id)+' '+resp+'\n')
else:
   for plate, text in pers_text_list:
      print_text(plate, text)
        
sys.stdout.write(order_resp.reason)

# check payment, if the order is paid, send json.post
if payment=='wayforpay':

  demand = json.loads(order_resp.text) 
  demand_href = (demand["meta"]["href"])
  agent_href = (demand["agent"]["meta"]["href"])

  ms_cashin= {
                "organization": {
                "meta": {
                  "href": "https://online.moysklad.ru/api/remap/1.2/entity/organization/0cad5148-5057-11eb-0a80-077800211373",
                  "metadataHref": "https://online.moysklad.ru/api/remap/1.2/entity/organization/metadata",
                  "type": "organization",
                  "mediaType": "application/json"
                }
              },
              "sum": total_sum,
              "agent": {
                "meta": {
                  "href": agent_href,
                  "metadataHref": "https://online.moysklad.ru/api/remap/1.2/entity/counterparty/metadata",
                  "type": "counterparty",
                  "mediaType": "application/json"
                }
              },
              "operations": [
              {
                "meta": {
                  "href": demand_href,
                  "metadataHref": "https://online.moysklad.ru/api/remap/1.2/entity/demand/metadata",
                  "type": "demand",
                  "mediaType": "application/json",
                  },
                "linkedSum": total_sum
              }
            ]
            }
  cashin=requests.post('https://online.moysklad.ru/api/remap/1.2/entity/cashin', json=ms_cashin, auth=a_auth)
