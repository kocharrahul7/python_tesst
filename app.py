# import the Flask class from the flask module
import sqlite3 as db
from sqlite3 import Error
from flask import Flask, render_template, redirect,url_for,request
from datetime import datetime
from datetime import date
from pandas import DataFrame, Series
import numpy as np
import pandas as pd
from nsepy import get_history
from bokeh.plotting import figure, output_file, show
from bokeh.embed import components
from bokeh.palettes import Blues9

# create the application object
app = Flask(__name__, template_folder='static/src')

def df2sqlite(dataframe, db_name = "import.sqlite", tbl_name = "import"):

	import sqlite3
	conn=sqlite3.connect(db_name)
	cur = conn.cursor()                                 

	wildcards = ','.join(['?'] * len(dataframe.columns))              
	data = [tuple(x) for x in dataframe.values]

	cur.execute("drop table if exists %s" % tbl_name)

	col_str = '"' + '","'.join(dataframe.columns) + '"'
	cur.execute("create table %s (%s)" % (tbl_name, col_str))

	cur.executemany("insert into %s values(%s)" % (tbl_name, wildcards), data)

	conn.commit()
	conn.close()


def create_table(create_table_sql,startdate,enddate):
	""" create a table from the create_table_sql statement
	:param conn: Connection object
	:param create_table_sql: a CREATE TABLE statement
	:return:
	"""
	niftyIT = get_history(symbol="NIFTYIT",
		start = startdate, 
		end=enddate,
		index=True)
	print(type(niftyIT))
	conn = db.connect('test.db')
	 
	try:
		c = conn.cursor()
		c.execute(create_table_sql)
		df2sqlite(niftyIT,'test.db','niftyIT')
		table = pd.read_sql_query("SELECT * FROM niftyIT", conn)
		print(table)
		return table
	except Error as e:
			print("Error: ",e)

def movingaverage(interval, window_size):
	window= np.ones(int(window_size))/float(window_size)
	return np.convolve(interval, window, 'same')

def create_connection(startdate,enddate):
	""" create a database connection to a database that resides
		in the memory
	"""
	print("sqlite3",db.version)
	sql_create_projects_table = """ CREATE TABLE IF NOT EXISTS niftyIT (
												timestmp integer PRIMARY KEY,
												Symbol	text NOT NULL,
												Expiry	text,
												High	text,
												Low		text,
												Close	text,
												Last	text
										  ); """
	# create projects table
	return create_table(sql_create_projects_table,startdate,enddate)

# use decorators to link the function to a url
@app.route('/')
def home():
	return "Hello, World!"  # return a string


@app.route('/welcome')
def welcome():
	return render_template('welcome.html')  # render a template

@app.route('/login', methods = ['GET','POST'])
def login():
	error = None
	if request.method=='POST':
		if request.form['username'] != 'admin' or request.form['password'] != 'admin':
			error = 'Invalid username/password. Please try again!'
		else:
			return redirect(url_for('start'))

	return render_template('login.html',error=error)  # render a template import stylesheet from coreUI

@app.route('/start', methods = ['GET','POST'])
def start():
	error = None
	if request.method=='POST':
		if "2015-01-01" >= request.form['stDate']  >= "2016-12-31" or "2015-01-01" >= request.form['enDate']  >= "2016-12-31":
			error = 'Invalid dates entered. Please try again!'
		else:
			startdate = request.form['stDate']
			enddate = request.form['enDate']
			date_format = '%Y-%m-%d'
			startdate = datetime.strptime(startdate, date_format)
			enddate = datetime.strptime(enddate, date_format)
			table = create_connection(startdate,enddate)
			table['movingaverage'] = table['Close'].rolling(window=260).mean()

			temp = []
			ls = []
			temp = ((table['Volume']  - table['Volume'].shift())/table['Volume'])*100
			for i in temp:
				if abs(i)>10:
					ls.append(1)
				else:
					ls.append(0)
			table['Volumeshocks'] = ls
			temp = []
			ls = [] 
			temp = ((table['Close']  - table['Close'].shift())/table['Close'])*100
			for i in temp:
				if abs(i)>2:
					ls.append(i/abs(i))
				else:
					ls.append(0)
			table['pricingshocks'] = ls
			table['pricingBlackSwan'] = ls
			ls = []

			for index, row in table.iterrows():
				if(row['Volumeshocks']==0 and row['pricingshocks']==1):
					ls.append(0)
				else:
					ls.append(1)

			table['PricingShockWithoutVolumeShock'] = ls
			
			print(table.to_string())
			
			p = figure()

			p.line(range(0, len(table['Close'])), table['Close'], line_width=2,line_color="#0000ff")
			script, div = components(p)
			return render_template("show.html", script=script, div=div)
			print(table.to_string())
			return table.to_string()
	return render_template('start.html')  # render a template import stylesheet from coreUI




# start the server with the 'run()' method
if __name__ == '__main__':
	app.run(debug=True)