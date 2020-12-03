from model import *

def DB():
	key=command()

	if key == 'random': dbRandom()
	elif key=='delete': dbDelete()
	elif key =='insert': dbInsert()
	elif key == 'search': dbSearch()
	elif key =='update': dbUpdate()