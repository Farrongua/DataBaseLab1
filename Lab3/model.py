import psycopg2
from psycopg2 import errors
import sys
import time


def ColumnData(table_name,column_name):
	conect=psycopg2.connect(dbname='DimaDB',user='postgres',password='dertyloik',host='localhost',port=5432)
	conect.set_session(autocommit=True)
	cursor=conect.cursor()

	try:
		cursor.execute(f"SELECT {column_name} FROM {table_name}")
		data=cursor.fetchall()
		print(data)
	except psycopg2.Error as error:
		print(error.pgcode)
		print(f'{error}')
		sys.exit()
	
	cursor.close()
	conect.close()

def dbSearch():
	conect=psycopg2.connect(dbname='DimaDB',user='postgres',password='dertyloik',host='localhost',port=5432)
	conect.set_session(autocommit=True)
	cursor=conect.cursor()

	n = int(input("Input quantity of attributes to search by : "))
	column=[]
	for h in range(0,n):
		column.append(str(input(f"Input name of the attribute number {h+1} to search by : ")))
	print(column)
	tables = []
	types = []
	if n == 2:
		curso_names_str = f"SELECT table_name FROM INFORMATION_SCHEMA.COLUMNS WHERE information_schema.columns.column_name LIKE '{column[0]}' INTERSECT ALL SELECT table_name FROM information_schema.columns WHERE information_schema.columns.column_name LIKE '{column[1]}'"
	else:
		curso_names_str = "SELECT table_name FROM INFORMATION_SCHEMA.COLUMNS WHERE information_schema.columns.column_name LIKE '{}'".format(column[0])
	print("\ncol_names_str:", curso_names_str)
	cursor.execute(curso_names_str)
	curso_names = (cursor.fetchall())
	for tup in curso_names:
		tables += [tup[0]]
	if 'student_teacher' in tables:
		tables.remove('student_teacher')
		print(tables)
	for s in range(0,len(column)):
		for k in range(0,len(tables)):
			cursor.execute(f"SELECT data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='{tables[k]}' AND column_name ='{column[s]}'")
			type=(cursor.fetchall())
			for j in type:
				types+=[j[0]]
	print(types)
	if n == 1:
		if len(tables) == 1:
			if types[0] == 'character varying':
				i_char = input(f"Input string for {column[0]} to search by : ")
				start=time.time()
				cursor.execute(f"SELECT * FROM {tables[0]} WHERE {column[0]} LIKE '{i_char}'")
				print(cursor.fetchall())
				print("request time: %s seconds"%(time.time()-start))
			elif types[0] == 'integer':
				left_limits = input("Enter left limit")
				right_limits = input("Enter right limit")
				start=time.time()
				cursor.execute(f"SELECT * FROM {tables[0]} WHERE {column[0]}>='{left_limits}' AND {column[0]}<'{right_limits}'")
				print(cursor.fetchall())
				print("request time: %s seconds"%(time.time()-start))
		elif len(tables) == 2:
			if types[0] == 'character varying':
				i_char = input(f"Input string for {column[0]} to search by : ")
				start = time.time()
				cursor.execute(f"SELECT {column[0]} FROM {tables[0]} WHERE {column[0]} LIKE '{i_char}' UNION ALL SELECT {column[0]} FROM {tables[1]} WHERE {column[0]} LIKE '{i_char}'")
				print(cursor.fetchall())
				print("request time: %s seconds" % (time.time() - start))
			elif types[0] == 'integer':
				left_limits = input("Enter left limit")
				right_limits = input("Enter right limit")
				start = time.time()
				cursor.execute(f"SELECT {column[0]} FROM {tables[0]} WHERE {column[0]}>='{left_limits}' AND {column[0]}<'{right_limits}' UNION ALL SELECT {column[0]} FROM {tables[1]} WHERE {column[0]}>='{left_limits}' AND {column[0]}<'{right_limits}' ")
				print(cursor.fetchall())
				print("request time: %s seconds" % (time.time() - start))

	elif n == 2:
		if len(tables) == 1:
			if types[0] == 'character varying' and types[1] == 'character varying':
				i_char = input(f"Input string for {column[0]} to search by : ")
				o_char = input(f"Input string for {column[1]} to search by : ")
				start = time.time()
				cursor.execute(f"SELECT * FROM {tables[0]} WHERE {column[0]} LIKE '{i_char}' AND {column[1]} LIKE '{o_char}' ")
				print(cursor.fetchall())
				print("request time: %s seconds" % (time.time() - start))
			elif types[0] == 'character varying' and types[1] == 'integer':
				i_char = input(f"Input string for {column[0]} to search by : ")
				left_limit = input(f"Enter left limit for {column[1]}")
				right_limit = input(f"Enter right limit for {column[1]}")
				start = time.time()
				cursor.execute(f"SELECT * FROM {tables[0]} WHERE {column[0]} LIKE '{i_char}' AND {column[1]}>='{left_limit}' AND {column[1]}<'{right_limit}'")
				print(cursor.fetchall())
				print("request time: %s seconds" % (time.time() - start))
			elif types[0] == 'integer' and types[1] == 'character varying':
				left_limit = input(f"Enter left limit for {column[0]}")
				right_limit = input(f"Enter right limit for {column[0]}")
				i_char = input(f"Input string for {column[1]} to search by : ")
				start = time.time()
				cursor.execute(f"SELECT * FROM {tables[0]} WHERE {column[0]}>='{left_limit}' AND {column[0]}<'{right_limit}' AND {column[1]} LIKE '{i_char}'")
				print(cursor.fetchall())
				print("request time: %s seconds" % (time.time() - start))
			elif types[0] == 'integer' and types[1] == 'integer':
				left_limit = input(f"Enter left limit for {column[0]} ")
				right_limit = input(f"Enter right limit for {column[0]} ")
				leftLimit = input(f"Enter left limit for {column[1]} ")
				rightLimit = input(f"Enter right limit for {column[1]} ")
				start = time.time()
				cursor.execute(f"SELECT * FROM {tables[0]} WHERE {column[0]}>='{left_limit}' AND {column[0]}<'{right_limit}' AND {column[1]}>='{leftLimit}' AND {column[1]}<'{rightLimit}' ")
				print(cursor.fetchall())
				print("request time: %s seconds" % (time.time() - start))
	
	cursor.close()
	conect.close()

	def dbInsert():
		conect=psycopg2.connect(dbname='DimaDB',user='postgres',password='dertyloik',host='localhost',port=5432)
		conect.set_session(autocommit=True)
		cursor=conect.cursor()

	table_name=input("Input table name > ")
	count=0
	values=[]

	if table_name =='order_products':
		print('Enter 3 value :')
		while(count<3):
			value=input()
			values.append(value)
			count+=1
		try:
			cursor.execute(f'INSERT INTO {table_name} VALUES ( "{values[0]}" , "{values[1]}" , "{values[2]}" )')
		except psycopg2.Error as error:
			print(error.pgcode)
			print(f'{error}')

	elif table_name == 'customer' or 'order' or 'product':
		print('Enter 2 value :')
		while(count<2):
			value=input()
			values.append(value)
			count+=1
		if table_name == 'product':
			print(f'INSERT INTO {table_name} VALUES ( {values[0]} , {values[1]} )')
			try:
				cursor.execute(f'INSERT INTO {table_name} VALUES ( {values[0]} , {values[1]} )')
			except psycopg2.Error as error:
				print(error.pgcode)
				print(f'{error}')
		elif table_name == 'order':
			print(f'INSERT INTO {table_name} VALUES ( {values[0]} , "{values[1]}" )')
			try:
				cursor.execute(f'INSERT INTO {table_name} VALUES ( {values[0]} , "{values[1]}" )')
			except psycopg2.Error as error:
				print(error.pgcode)
				print(f'{error}')
		elif table_name == 'customer':
			print(f'INSERT INTO {table_name} VALUES ( "{values[0]}" , {values[1]} )')
			try:
				cursor.execute(f'INSERT INTO {table_name} VALUES ( "{values[0]}" , {values[1]} )')
			except psycopg2.Error as error:
				print(error.pgcode)
				print(f'{error}')
	else: print("error table name")

	cursor.close()
	conect.close()

def dbUpdate():
	conect=psycopg2.connect(dbname='DimaDB',user='postgres',password='dertyloik',host='localhost',port=5432)
	conect.set_session(autocommit=True)
	cursor=conect.cursor()

	table_name=input('Input table name : ')
	column_name=input('Input column name : ')
	ColumnData(table_name,column_name)
	old_name=input('Input old_name : ')
	new_name=input('Input new_name : ')

	if table_name == 'customer':
		try:
			if column_name == 'data':
				cursor.execute(f'UPDATE {table_name} SET {column_name} = "{new_name}" WHERE {column_name}= "{old_name}" ')
			elif column_name == 'customer_id':
				cursor.execute(f'UPDATE {table_name} SET {column_name} = {new_name} WHERE {column_name}= {old_name} ')
		except psycopg2.Error as error:
			print(error.pgcode)
			print(f'{error}')
	elif table_name == 'order':
		try:
			if column_name == 'order_id':
				cursor.execute(f'UPDATE {table_name} SET {column_name} = {new_name} WHERE {column_name}= {old_name} ')
			elif column_name == 'user_data':
				cursor.execute(f'UPDATE {table_name} SET {column_name} = "{new_name}" WHERE {column_name}= "{old_name}" ')
		except psycopg2.Error as error:
			print(error.pgcode)
			print(f'{error}')
	elif table_name == 'product' or 'order_products':
		try:
			cursor.execute(f'UPDATE {table_name} SET {column_name} = {new_name} WHERE {column_name}= {old_name} ')
		except psycopg2.Error as error:
			print(error.pgcode)
			print(f'{error}')

	cursor.close()
	conect.close()


def dbDelete():
	conect=psycopg2.connect(dbname='DimaDB',user='postgres',password='dertyloik',host='localhost',port=5432)
	conect.set_session(autocommit=True)
	cursor=conect.cursor()

	table_name=input("Input table name : ")
	column_name=input('Enter column name : ')
	ColumnData(table_name,column_name)

	value=input('Enter value : ')
	try:
		if column_name == 'user_data' or 'data':
			cursor.execute(f'DELETE FROM {table_name} WHERE {column_name} = "{value}" ')
		else:
			cursor.execute(f'DELETE FROM {table_name} WHERE {column_name} = {value} ')
	except psycopg2.Error as error:
		print(error.pgcode)
		print(f'{error}')

	cursor.close()
	conect.close()

def dbRandom():
	conect=psycopg2.connect(dbname='DimaDB',user='postgres',password='dertyloik',host='localhost',port=5432)
	conect.set_session(autocommit=True)
	cursor=conect.cursor()

	table_name=input("Input table name > ")
	size=input('Random size :')
	if table_name =='order':
		print(f'INSERT INTO order SELECT (trunc(65+random()*54000)::int),chr(trunc(65+random()*54000)::int) FROM generate_series(1,{size})')
		cursor.execute(f'INSERT INTO order SELECT (trunc(65+random()*54000)::int),chr(trunc(65+random()*54000)::int) FROM generate_series(1,{size})')
	elif table_name =='product':
		cursor.execute(f'INSERT INTO product SELECT (trunc(65+random()*54000)::int),(trunc(65+random()*54000)::int) FROM generate_series(1,{size})')
	elif table_name =='customer':
		cursor.execute(f'INSERT INTO customer SELECT chr(trunc(65+random()*54000)::int),(trunc(65+random()*54000)::int) FROM generate_series(1,{size})')
	elif table_name =='order_products':
		cursor.execute(f'INSERT INTO order_products SELECT (trunc(65+random()*54000)::int),(trunc(65+random()*54000)::int),(trunc(65+random()*54000)::int) FROM generate_series(1,{size})')
	
	cursor.close()
	conect.close()