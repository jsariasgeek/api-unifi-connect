import pymongo
from flask import Flask, jsonify, request
from datetime import datetime
from time import mktime

def get_db_connection(uri):
	client = pymongo.MongoClient(uri)
	return client.ace


app = Flask(__name__)
db = get_db_connection('mongodb://localhost:27117')

def date_to_unix_timestamp(date):
	'''this function converts a date in format YYYY-MM-DD-HH-mm in unix timestamp'''
	date_splitted = date.split('-')
	year = date_splitted[0]
	month = date_splitted[1]
	day = date_splitted[2]
	hour = date_splitted[3]
	minute = date_splitted[4]

	date_time = datetime(int(year), int(month), int(day), int(hour), int(minute))
	# Substract the UTC-5 timezone
	return mktime(date_time.timetuple()) - 18000


@app.route('/get-total-conn', methods=['POST'])
def get_total_conn():
	'''
	This function return the total conns in ap for between
	start and date time.
	:return:
	example json
	{
	"ap_mac":"18:e8:29:59:cf:8b",
	"start":"2019-10-04-00-00",
	"end":"2019-11-05-00-00"
	}
	'''

	content = request.json

	ap_mac = content['ap_mac']
	start = date_to_unix_timestamp(content['start'])
	end = date_to_unix_timestamp(content['end'])

	report = list(db.guest.find(
		{
			"ap_mac":ap_mac,
			"start":{"$gte":start, "$lte":end}
		},
		{"_id":0,"mac":1, "start":1, "end":1, "duration":1, "bytes":1, "rx_bytes":1, "tx_bytes":1}
	))

	return jsonify({'report':report})


@app.route('/get-data-downloaded', methods=['POST'])
def get_data_downloaded():
	'''
	This function get a report of data downloaded for a list of mac
	addresses during a period of time
	parameters:
	start: Start Date. Format: YYYY-MM-DD-HH-mm
	end: End Date. Format: YYYY-MM-DD-HH-mm
	ap_list: list of mac address to return data
	ex. json parameter
	{
	"ap_macs":[
		"18:e8:29:59:cf:8b",
		"18:e8:29:59:cf:e4",
		"18:e8:29:a0:d5:d0",
		"18:e8:29:a3:7c:df",
		"18:e8:29:a3:7d:7c",
		"18:e8:29:a3:7d:ed",
		"18:e8:29:a3:7e:5b"
	],
	"start":"2019-10-04-00-00",
	"end":"2019-11-05-00-00"
	}
	'''

	content = request.json
	ap_macs = content['ap_macs']
	start = date_to_unix_timestamp(content['start'])
	end = date_to_unix_timestamp(content['end'])
	# except KeyError:
	# 	return 'Please provide full information'

	report = list(db.guest.aggregate(
		[{"$match": {"start": {"$gte": start, "$lte": end}, "ap_mac": {"$in": ap_macs}}},
		 {"$group": {
			 "_id": "$ap_mac",
			 "data_download": {"$sum":"$tx_bytes"},
			 "data_upload":{"$sum": "$rx_bytes"},
			 "total_data":{"$sum": "$bytes"},
			 "total_conns": {"$sum": 1}
		 }}
		 ]))
	return jsonify({'report':report})


if __name__ == '__main__':
	app.run(debug=True, host='0.0.0.0')