"""
  This script follow up the process for extract, transform and load data.
"""

# standard imports
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

# sqlite imports
import sqlite3
from sqlite3 import Error

# logger
import logging
logging.basicConfig(
    format = '%(asctime)-5s %(name)-15s %(levelname)-8s %(message)s', 
    level  = logging.INFO, # Nivel de los eventos que se registran en el logger
)

class ETLPipeLine:

  def __init__(self, filename):
      self.filename = filename

  def extract(self):
    """
    :param: root to file name
    :type: str

    :return: A file 
    :rtype: Object Pandas
    """

    try:
      df = pd.read_csv(self.filename, delimiter = "\t", header = None)
      data = {'rut':[],
        'dv':[], 
        'nombre':[], 
        'apellido':[], 
        'genero':[], 
        'fecha_nacimiento':[], 
        'fecha_vencimiento':[], 
        'deuda':[], 
        'direccion':[], 
        'ocupacion':[],  
        'altura':[], 
        'peso':[], 
        'correo':[], 
        'estatus_contacto':[], 
        'prioridad':[], 
        'telefono':[], 
        'estatus_contacto':[], 
        'prioridad':[]}

      for index, row in df.iterrows():
        if len(row[0]) == 242:
          self.splitStructure(row[0], data)
      return pd.DataFrame(data)
    except Exception as err:
        print("Error, file '{}' does not exist!!!: {}".format(self.filename, err))
    
  # transform data
  def transform(self, data):
    """
		:param content: Data to transform
		:type content: DataFrame

		:return: customers
		:rtype: DataFrame

    :return: emails
		:rtype: DataFrame

    :return: phones
		:rtype: DataFrame
		"""

    #Define 3 structures to transform
    customers = {'fiscal_id':[], 'first_name':[], 'last_name':[], 'gender':[], 'birth_date':[], 'age':[], 'age_group':[], 'due_date':[], 'delinquency':[], 'due_balance':[], 'address':[], 'ocupation':[], 'best_contact_ocupation':[]}
    emails = {'fiscal_id':[], 'email':[], 'status':[], 'priority':[] }
    phones = {'fiscal_id':[], 'phone':[], 'status':[], 'priority':[]}
    formato = "%Y-%m-%d"

    # drop NaN values
    data.dropna(inplace=True)
    data = data.reset_index(drop=True)
    val = data.isnull().any()

    #string to timestamp
    data['fecha_nacimiento'] = list(map(self.time_stamp, data['fecha_nacimiento']))
		
		#string to timestamp
    data['fecha_vencimiento'] = list(map(self.time_stamp, data['fecha_vencimiento']))

    listBestCO = []
    for index, row in data.iterrows():
      # format Costumers
      edad = relativedelta(datetime.now(), row['fecha_nacimiento']) #age info
      delinquency = datetime.now() - row['fecha_vencimiento']
      #calculate best_contact_ocupation
      rut = row['rut']
      dv = row['dv']
      ocupation = row['ocupacion']

      #this part take along time
      if (rut+dv,ocupation) not in listBestCO: #only one customer per occupancy must have this field checked
        listBestCO.append((rut+dv,ocupation))
        best_contact_ocupation = self.getBestOcupation(rut, dv, ocupation, data)
      else:
        best_contact_ocupation = 0

      customers['fiscal_id'].append(row['rut']+row['dv'])
      customers['first_name'].append(row['nombre'].upper() if len(row['nombre'])!=0 else None)
      customers['last_name'].append(row['apellido'].upper() if len(row['apellido'])!=0 else None)
      customers['gender'].append(row['genero'].upper() if len(row['genero'])!=0 else None)
      customers['birth_date'].append(row['fecha_nacimiento'].strftime(formato))
      customers['age'].append(int(edad.years))
      customers['age_group'].append(self.ageGroup(int(edad.years)))
      customers['due_date'].append(row['fecha_vencimiento'].strftime(formato))
      customers['delinquency'].append(int(delinquency.days))
      customers['due_balance'].append(int(row['deuda']) if len(row['deuda'])!=0 else None)
      customers['address'].append(row['direccion'].upper() if len(row['direccion'])!=0 else None)
      customers['ocupation'].append(row['ocupacion'].upper() if len(row['ocupacion'])!=0 else None)
      customers['best_contact_ocupation'].append(best_contact_ocupation)

      # format emails
      emails['fiscal_id'].append(row['rut']+row['dv'])
      emails['email'].append(row['correo'].upper() if len(row['correo'])!=0 else None)
      emails['status'].append(row['estatus_contacto'].upper() if len(row['estatus_contacto'])!=0 else None)
      emails['priority'].append(int(row['prioridad']) if row['prioridad'].isdigit() else None)

      #format phones
      phones['fiscal_id'].append(row['rut']+row['dv'])
      phones['phone'].append(row['telefono'] if len(row['telefono'])!=0 else None)
      phones['status'].append(row['estatus_contacto'].upper() if len(row['estatus_contacto'])!=0 else None)
      phones['priority'].append(int(row['prioridad']) if row['prioridad'].isdigit() else None)

    customers = pd.DataFrame(customers)
    customers.to_excel('output/customers.xlsx', index=False)

    emails = pd.DataFrame(emails)
    emails.to_excel('output/emails.xlsx', index=False)

    phones = pd.DataFrame(phones)
    phones.to_excel('output/phones.xlsx', index=False)
    return customers, emails, phones

  # load data
  def load(self, customers, emails, phones):
    """
		:param data: A DataFrame to load into a table of dataBase.db3
		:type data: DataFrame

		:return: None
		:rtype: None
		"""
    try:
      db = Database()
      conn = db.connection()

      # load customers into table consumers of database.db3
      queryCustomers = "CREATE TABLE IF NOT EXISTS \"customers\" (\"fiscal_id\"	TEXT,\"first_name\"	TEXT,\"last_name\"	TEXT,\"gender\"	TEXT,\"birth_date\"	TEXT,\"age\"	INTEGER,\"age_group\"	INTEGER,\"due_date\"	TEXT,\"delinquency\"	INTEGER,\"due_balance\"	INTEGER,\"address\"	TEXT,\"ocupation\"	TEXT,\"best_contact_ocupation\"	INTEGER,PRIMARY KEY(\"fiscal_id\"));"
      db.createTable(conn,queryCustomers)
      #customers = customers.drop_duplicates()
      dataCustomers = customers.to_numpy().tolist()
      db.bulkData(conn, 'customers', 'VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',dataCustomers)

      queryEmails = "CREATE TABLE IF NOT EXISTS \"emails\" (\"fiscal_id\"	TEXT,\"email\"	TEXT,\"status\"	TEXT,\"priority\"	INTEGER,PRIMARY KEY(\"fiscal_id\",\"email\"));"
      db.createTable(conn,queryEmails)
      dataEmails = emails.to_numpy().tolist()
      db.bulkData(conn, 'emails', 'VALUES(?, ?, ?, ?)',dataEmails)

      queryPhones = "CREATE TABLE IF NOT EXISTS \"phones\" (\"fiscal_id\"	TEXT,\"phone\"	TEXT,\"status\"	TEXT,\"priority\"	INTEGER,PRIMARY KEY(\"fiscal_id\",\"phone\"));"
      db.createTable(conn, queryPhones)
      dataPhones = phones.to_numpy().tolist()
      db.bulkData(conn, 'phones', 'VALUES(?, ?, ?, ?)',dataPhones)

    except Exception as err:
      print("An error has ocurred...{}".format(err))
    finally:
      db.connectionClose(conn)

  def ageGroup(self, age):
    if age <= 20:
      group = 1
    if age >= 21 and age <= 30:
      group = 2
    if age >= 31 and age <= 40:
      group = 3
    if age >= 41 and age <= 50:
      group = 4
    if age >= 51 and age <= 60:
      group = 5
    if age >= 61:
      group = 6
    return group

  def time_stamp(self, dates):
    """
    :param dates:
    :type dates:

    :return:
    :rtype:
    """

    item = dates.split('-')
    formato = "%Y-%m-%d"

    if len(item) < 3:
      # item is a list with year, month and day, if there no one of these, then timestamp = current day
      timestamp = datetime.timestamp(datetime.now())

    else:
      # year has four digits, otherwise timestamp := current day
      if len(item[0]) != 4:
        timestamp = datetime.timestamp(datetime.now())

      # month has two digits, otherwise timestamp := current day
      elif len(item[1]) != 2:
        timestamp = datetime.timestamp(datetime.now())
      
      # day has two digits, otherwise timestamp := current day
      elif len(item[2]) != 2:
        timestamp = datetime.timestamp(datetime.now())

      else:
        timestamp = datetime.strptime(dates, formato)

    return timestamp
  
  def splitStructure(self, line, data):
      """
      :param: line of file
      :type: str

      :param: data
      :type: dict
      
      this function applies the split of each row in the txt file according to 
          the delimitation mentioned in the test and add to the main dataframe and
          then put into data
      """
      
      data['rut'].append(line[0:7].replace(' ', ''))
      data['dv'].append(line[7:8])
      data['nombre'].append(line[8:28].replace(' ', ''))
      data['apellido'].append(line[28:53].replace(' ', ''))
      data['genero'].append(line[53:62].replace(' ', ''))
      data['fecha_nacimiento'].append(line[62:72].replace(' ', ''))
      data['fecha_vencimiento'].append(line[72:82].replace(' ', ''))
      data['deuda'].append(line[82:88].replace(' ', ''))
      data['direccion'].append(line[88:138].split('  ')[0])
      data['ocupacion'].append(line[138:168].split('  ')[0])
      data['altura'].append(line[168:172].replace(' ', ''))
      data['peso'].append(line[172:174].replace(' ', ''))
      data['correo'].append(line[174:224].replace(' ', ''))
      data['estatus_contacto'].append(line[224:232].replace(' ', ''))
      data['prioridad'].append(line[232:233].replace(' ', ''))
      data['telefono'].append(line[233:242].replace(' ', ''))
  
  def getBestOcupation(self, rut, dv, ocupation, data):
    """
      :param: rut
      :type: str

      :param: dv
      :type: str

      :param: ocupation
      :type: str

      :return: 
		  :rtype: int

      filter data by values in rut, dv and occupation. 
        Then it calculates how many validated telephones there are in this filter.
    """
    filtroRUT = data[data['rut'] == rut]
    filtroDV = filtroRUT[filtroRUT['dv'] == dv]
    filtroOCUPATION = filtroDV[filtroDV['ocupacion'] == ocupation]
    filtroVALID = filtroOCUPATION[filtroOCUPATION['estatus_contacto'] == 'Valido']
    
    phonesValids = filtroVALID.groupby("rut")["telefono"].apply(set).reset_index()['telefono']
    if phonesValids.shape[0] != 0:
        #print(phonesValids[0])
        best_contact_ocupation = 1 if len(phonesValids[0])>1 else 0
    else:
        best_contact_ocupation = 0
    return best_contact_ocupation
  
  def run_etl(self):
    """
		:param: None
		:rtype: None

		:return: None
		:rtype: None
		"""

    # extract
    logging.info(' \n Starting data extraction \n')
    data = self.extract()
    logging.info(' \n Extraction completed \n \n')
    
    logging.info(' \n Starting data transformation \n')
    customers, emails, phones = self.transform(data)
    logging.info(' \n Transformation completed \n \n')
    
    logging.info(' \n Starting data load \n')
    self.load(customers, emails, phones)
    logging.info(' \n Load completed \n \n')

class Database:

  def connection(self):
    try:
      # Connect to the database
        con = sqlite3.connect('database.db3')

        return con
    except Error:
        print(Error)
  
  def connectionClose(self, con):
    con.close()

  def createTable(self, con, query):
    """
    :param: con
    :type: sqlite3 connection

    :param: query : to create a table int con database
    :type: str
    """
    cursorObj = con.cursor()
    cursorObj.execute(query)
    con.commit()

  def insert(self, con, table, valuesFormat, entities):
    """
    :param: con
    :type: sqlite3 connection

    :param: table : table into database con
    :type: str

    :param: entites : list of values to insert into table param
    :type: list
    """
    cursorObj = con.cursor()
    
    cursorObj.execute('INSERT OR IGNORE INTO ' + table + ' ' + valuesFormat, entities)
    
    con.commit()
  
  def bulkData(self, con, table, valuesFormat, data):
    """
    :param: con
    :type: sqlite3 connection

    :param: table : table into database con
    :type: str

    :param: entites : list of values to insert into table param
    :type: list
    """
    con.execute("PRAGMA synchronous=OFF")
    con.execute("BEGIN TRANSACTION")
    cursorObj = con.cursor()
    # ignore was added because there are repeated fiscal_id and it was requested that this be a primary key in all 3 tables.
    cursorObj.executemany("INSERT OR IGNORE INTO "+ table + " " + valuesFormat, data)
    con.commit()

if __name__=='__main__':

  _filename = input("\n \n Enter the file path: ")
  #_filename = 'customers.txt'
  
  pipe = ETLPipeLine(filename=_filename)
  pipe.run_etl()