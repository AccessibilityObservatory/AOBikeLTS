import psycopg2
import psycopg2.extras
import os
import argparse
import datetime
import sys
from ConfigParser import SafeConfigParser
from AODB import AODB


#This script assigns an LTS score to each OpenStreetMap way. The LTS scores, along with which rule assigned the LTS score, are stored in the database in the osm.ways table.

config = SafeConfigParser()
config.read(os.path.expanduser("~/.aoconfig"))

bikelts_dsn = "host={} dbname={} user={} password={}".format(config.get("LTS","host"),
                                                           config.get("LTS","dbname"),
                                                           config.get("LTS","user"),
                                                           config.get("LTS","password"))

aodb_dsn = "host={} dbname={} user={} password={}".format(config.get("aodb","host"),
                                                           config.get("aodb","dbname"),
                                                           config.get("aodb","user"),
                                                           config.get("aodb","password"))


aodb = AODB()

class Timer:

    def __init__(self):
        self.start_time = datetime.datetime.now()
    
    def elapsed(self):
        return datetime.datetime.now() - self.start_time

def get_county_geom(con, countyid):
	query = """SELECT ST_AsText(geom)
				FROM zones.counties c
				WHERE c.id = %s
				;"""

	with con.cursor() as cur:
		cur.execute(query, (countyid,))
		results = cur.fetchone()

	return results

def get_cbsa_geom(con, cbsaid):
	query = """SELECT ST_AsText(ST_Buffer_Meters(geom, 15000))
				FROM zones.cbsas c
				WHERE c.id = %s
				;"""

	with con.cursor() as cur:
		cur.execute(query, (cbsaid,))
		results = cur.fetchone()

	return results

def get_area_bounds_cbsa(con, cbsaid):
	query = """WITH cbsa AS (SELECT ST_Buffer_Meters(ST_Envelope(c.geom), 25000) AS geom
							FROM zones.cbsas c
							WHERE c.id = %s)
				SELECT ST_XMin(c.geom),
						ST_YMin(c.geom),
						ST_XMax(c.geom),
						ST_YMax(c.geom)
				FROM cbsa c;"""

	with con.cursor() as cur:
		cur.execute(query, (cbsaid,))
		results = cur.fetchone()

	return results

def get_osm_ways(con, con2, countyid):
	query = """SELECT w.id, w.tags
				FROM osm.ways w
				WHERE ST_Intersects(w.bbox, ST_GeomFromText(%s, 4326));"""

	with con.cursor() as cur:
		cur.execute(query, (get_county_geom(con2, countyid)))
		results = cur.fetchall()

	return [x for x in results]

def get_osm_ways_cbsa(con, con2, cbsaid):
	query = """SELECT w.id, w.tags
				FROM osm.ways w
				WHERE ST_Intersects(w.bbox, ST_GeomFromText(%s, 4326));"""

	with con.cursor() as cur:
		cur.execute(query, (get_cbsa_geom(con2, cbsaid)))
		results = cur.fetchall()

	return [x for x in results]

def get_osm_nodes(con, con2, countyid):
	query = """SELECT n.id, n.tags
				FROM osm.nodes n
				WHERE ST_Intersects(ST_GeomFromText(%s, 4326), n.geom);"""

	with con.cursor() as cur:
		cur.execute(query, (get_county_geom(con2, countyid)))
		results = cur.fetchall()

	return [x for x in results]

def get_osm_nodes_cbsa(con, con2, cbsaid):
	query = """SELECT n.id, n.tags
				FROM osm.nodes n
				WHERE ST_Intersects(ST_GeomFromText(%s, 4326), n.geom);"""

	with con.cursor() as cur:
		cur.execute(query, (get_cbsa_geom(con2, cbsaid)))
		results = cur.fetchall()

	return [x for x in results]
 
def get_ozones(con, stateid):
	ozones = aodb.ozones_for_state(stateid)
	print(len(ozones))

	return [x[0] for x in ozones]

def execute_query(con, query, way, LTS_rank, assignment_code):
	with con.cursor() as cur:
		cur.execute(query, (way,))
		result = cur.fetchone()[0]

	if result == True:
		return (LTS_rank, assignment_code)


def get_tags_for_way(con, way):
	query = """SELECT w.tags
				FROM osm.ways w
				WHERE w.id = %s;"""

	with con.cursor() as cur:
		cur.execute(query, (way,))
		results = cur.fetchone()

	return results[0]

def get_tags_for_node(con, node):
	query = """SELECT n.tags
				FROM osm.nodes n
				WHERE n.id = %s;"""

	with con.cursor() as cur:
		cur.execute(query, (node,))
		results = cur.fetchone()

	return results[0]

def get_ways_for_node(con, node):
	query = """SELECT wn.way_id, w.tags
				FROM osm.way_nodes wn
				LEFT JOIN osm.ways w
				ON wn.way_id = w.id
				WHERE wn.node_id = %s"""

	with con.cursor() as cur:
		cur.execute(query, (node,))
		results = cur.fetchall()

	return [x for x in results]

def assign_way_LTS(con, tags):
	#Assign an LTS score based on the tag logic. The queries generally progress from lower-stress to higher-stress, and the function returns tuples of LTS scores & assignment codes.
	#This function also generally proceeds from most specific to least specific, and bails out as it reaches a criteria match.

	try:

		#extract the max speed of the roadway segment
		try:
			maxspeed = int(tags.get('maxspeed','')[0:2] or 0)
		except ValueError, e:
			print('Way has non-numerical max speed tag. Error: {}'.format(e))
			maxspeed = 0

		#extract the number of lanes
		try:
			lanes = int(tags.get('lanes','')[0:1] or 0)
			if tags.get('oneway', '') == 'yes':
				lanes_each_way = lanes
			else:
				lanes_each_way = lanes/2
		except ValueError, e:
			print('Way has non-numerical lane number tag. Error: {}'.format(e))
			lanes = 0

		#0 - highway = service should be discarded -- these are alleyways, parking lots, etc. These are given an LTS rank of 0, indicating no routing should occur.
		#don't route on segments under construction
		#don't route on tracks -- agricultural or forest paths
		#don't route on raceways -- go kart, etc.
		#don't route on bridleways
		#don't route on motorways
		#unless bicycle access is specifically designated -- indicating the way is designed for bike use
		if tags.get('highway','') in ['service', 'construction', 'track', 'raceway', 'bridleway', 'road', 'proposed', 'rest_area', 'platform', 'motorway', 'motorway_link', 'corridor'] and tags.get('bicycle','') not in ['yes','designated']:
			return (0, 0)

		#1 - footpaths/sidewalks that don't explicitly allow bicycles need to be discarded; if no bicycle tag, assumes disallowed

		if tags.get('highway','') in ['footway', 'pedestrian', 'steps'] and tags.get('bicycle','') not in ['yes','designated']:
			return (0, 1)

		#2 - generic paths that don't explicitly disallow bicycles should be included as LTS 1 -- this commonly includes suburban bicycle paths around lakes

		if tags.get('highway','') == 'path' and tags.get('bicycle','') not in ['no', 'permissive', 'dismount']:
			return (1, 2)

		#3 - generic paths that don't allow bicycles should be discarded
		if tags.get('highway','') == 'path' and tags.get('bicycle','') in ['no', 'permissive', 'dismount']:
			return (0, 3)

		#4 - crossings that don't disallow bikes are LTS 1
		if tags.get('highway','') == 'crossing' and tags.get('bicycle','') not in ['no', 'permissive', 'dismount']:
			return (1, 4)

		#5 - footpaths/sidewalks that do explicitly allow bicycles should be LTS 1

		if tags.get('highway','') in ['footway', 'pedestrian'] and tags.get('bicycle','') in ['yes', 'designated']:
			return (1, 5)

		#6 - restricted-access facilities with bicycle designation should be LTS 2
		if tags.get('access','') == 'no' and tags.get('bicycle') == 'designated':
			return (2, 6)

		#7 - fully separated facilities, LTS 1 
		if tags.get('cycleway:right','') == 'track' or tags.get('cycleway:left','') == 'track' or tags.get('cycleway','') == 'opposite_track' or tags.get('cycleway','') == 'track' or tags.get('highway','') == 'cycleway':
			return (1, 7)

		#buffered bike lanes are not yet implemented uniformly in OSM -- skip for now
		'''#buffered bike lane on a small, slower street, LTS 2
		query = """SELECT CASE WHEN (tags -> 'cycleway:right' = 'lane'
									AND tags -> 'cycleway:right:buffer' = 'yes')
								OR (tags -> 'cycleway:left' = 'lane'
									AND tags -> 'cycleway:left:buffer' = 'yes')
								AND tags -> left('maxspeed', 2)::int <= 30
								AND tags -> 'lanes'::int=1
								THEN TRUE
								ELSE FALSE
							END
					FROM osm.ways w
					WHERE w.id = %s;"""

		result = execute_query(con, query, way, 2, 2)

		if result:
			return result

		#buffered bike lane on a larger street with no parking, LTS 2
		query = """SELECT CASE WHEN (tags -> 'cycleway:right' = 'lane'
									AND tags -> 'cycleway:right:buffer' = 'yes')
								OR (tags -> 'cycleway:left' = 'lane'
									AND tags -> 'cycleway:left:buffer' = 'yes')
								AND tags -> left('maxspeed', 2)::int <= 30
								AND tags -> 'lanes'::int=1
								THEN TRUE
								ELSE FALSE
							END
					FROM osm.ways w
					WHERE w.id = %s;"""'''

		#8 shared busways, LTS 2
		if tags.get('cycleway','') == 'share_busway' or tags.get('cycleway','') == 'opposite_share_busway':
			return (2, 8)

		#9 low-speed shared lanes, LTS 2
		
		if tags.get('cycleway','') == 'shared_lane' and 0 < maxspeed <= 25:
			return (2, 9)

		#10 higher-speed / no speed info & non-residential shared lanes, LTS 3
		if tags.get('cycleway','') == 'shared_lane' and tags.get('highway','') != 'residential':
			return (3, 10)

		#11-19 different cases of on-street bike lanes
		if not set([tags.get('cycleway',''), tags.get('cycleway:left',''), tags.get('cycleway:right','')]).isdisjoint(['lane', 'opposite']):
			if 0 < lanes_each_way < 2:
				if 0 < maxspeed <= 25:
					return (1, 11)
				elif 0 < maxspeed <= 30:
					return (2, 12)
				elif maxspeed > 30:
					return (3, 13)

			elif lanes_each_way == 2:
				if 0 < maxspeed <= 25:
					return (2, 14)
				elif maxspeed > 25:
					return (3, 15)

			elif lanes_each_way > 2:
				if 0 < maxspeed <= 35:
					return (3, 16)
				elif maxspeed > 35:
					return (4, 17)

			#if insufficient speed or lane configuration information
			if tags.get('highway','') in ['unclassified', 'tertiary', 'tertiary_link']:
				return (2, 18)

			#if no assignment yet and has a bike lane, LTS 3 (e.g. secondary segments)
			return (3, 19)

		#20 - highway = residential or living_street, LTS 1
		if tags.get('highway','') in ['residential', 'living_street']:
			return (1, 20)

		#21 - small & slow (under 3 lanes & maxspeed <= 25), LTS 2
		if 0 < lanes <= 3 and 0 < maxspeed <= 25:
			return (2, 21)

		#22 -- slow but more than 3 lanes, LTS 3 -- informed by PFB
		if lanes > 3 and 0 < maxspeed <= 25:
			return (3, 22)

		#23 - slow and lanes not specified, LTS 2
		if 0 < maxspeed <= 25 and lanes==0:
			return(2, 23)

		#24 - highway = tertiary & no assignment yet (built in), LTS 3
		if tags.get('highway','') == 'tertiary':
			return (3, 24)

		#25 - highway = tertiary_link or unclassified & no assignment yet (built in), LTS 2
		if tags.get('highway','') in ['tertiary_link', 'unclassified']:
			return (2, 25)

		#26 - highway = primary, trunk, primary_link, trunk_link, & no assignment yet (no separated facilities), LTS 4
		if tags.get('highway','') in ['primary', 'primary_link', 'trunk', 'trunk_link']:
			return (4, 26)

		#27 - catch-all, if we reach this point with no assignment, LTS 4
		return(4, 27)

	except Exception, e:
		print(e)

def assign_node_LTS(con, id, tags): 
	ways = get_ways_for_node(con, id)
	way_ltsranks = sorted([way[1].get('ltsrank') for way in ways])

	if tags.get('highway','') == 'traffic_signals' or (tags.get('highway','')=='crossing' and tags.get('crossing', '') in ['traffic_signals', 'pelican', 'toucan', 'pegasus']):
		#assign node LTS rank as 1 -- assume signalized crossings pose no traffic stress to cyclists
		#ltsrank = int(way_ltsranks[0])
		return (1, 0)

	else:
		#assume unsignalized -- assign node LTS as the highest of all connecting ways
		#add error catching here
		try:
			ltsrank = int(way_ltsranks[-1])
			return (ltsrank, 1)
		except TypeError, e:
			print('Node assignment failed with error: {}. Assign rank 0 to discard.'.format(e))
			return (0, 0)


def update_osm_ways(con, way_data):
	query = """UPDATE osm.ways AS w
				SET tags = tags || hstore('ltsrank'::text, v.ltsrank::text) || hstore('ltscode'::text, v.ltscode::text)
				FROM (VALUES %s) AS v(id, ltsrank, ltscode)
				WHERE w.id = v.id;"""

	try:
		with con.cursor() as cur:
			psycopg2.extras.execute_values(cur, query, way_data)
			con.commit()
	except Exception, e:
		print(e)

def update_osm_nodes(con, node_data):
	query = """UPDATE osm.nodes AS n
				SET tags = tags || hstore('ltsrank'::text, v.ltsrank::text) || hstore('ltscode'::text, v.ltscode::text)
				FROM (VALUES %s) AS v(id, ltsrank, ltscode)
				WHERE n.id = v.id;"""

	try:
		with con.cursor() as cur:
			psycopg2.extras.execute_values(cur, query, node_data)
			con.commit()
	except Exception, e:
		print(e)

def process_state(con, con2, stateid):
	counties = aodb.counties_for_state(stateid)
	print('\nWorking on LTS assignments for state {}...'.format(stateid))
	t=Timer()

	for countyid in counties:
		t0 = t.elapsed()
		print('Working on county {}...'.format(countyid))
		ways = get_osm_ways(con, con2, countyid)

		nodes = get_osm_nodes(con, con2, countyid)

		way_data = []
		count=0
		tot_ways = len(ways)
		for way in ways:
			id, tags = way
			count+=1
			print('Processing way {}, {} of {}...'.format(id, count, tot_ways))
			LTSrank, assignment_code = assign_way_LTS(con, tags)
			way_data.append((id, LTSrank, assignment_code))

		update_osm_ways(con, way_data)

		node_data = []
		count=0
		tot_nodes = len(nodes)
		for node in nodes:
			id, tags = node
			count+=1
			print('Processing node {}, {} of {}...'.format(id, count, tot_nodes))
			LTSrank, assignment_code = assign_node_LTS(con, id, tags)
			node_data.append((id, LTSrank, assignment_code))

		update_osm_nodes(con, node_data)

		t1 = t.elapsed()
		print('\nProcessed ways and nodes for county {} in {}'.format(countyid, t1-t0))

	print('\nProcessed all counties for state {} in {}'.format(stateid, t.elapsed()))
		

def process_cbsa(con, con2, cbsaid):
	#ozones = get_ozones(con, stateid)
	#ozones=ozones[0:1]

	print('\nWorking on LTS assignments for cbsaid {}'.format(cbsaid))
	t = Timer()
	#ways = get_osm_ways_cbsa(con, con2, cbsaid)
	t0 = t.elapsed()
	#print('Fetched {} ways for cbsaid {} in {} ({} per second). Now processing ways...'.format(tot_ways, cbsaid, t0, tot_ways / t0.total_seconds()))
	#tot_ways = len(ways)

	nodes = get_osm_nodes_cbsa(con, con2, cbsaid)
	t0 = t.elapsed()
	tot_nodes = len(nodes)

	# print('Fetched {} nodes for cbsaid {} in {} ({} per second). Now processing ways...'.format(tot_nodes, cbsaid, t0, tot_nodes / t0.total_seconds()))
	
	# count=0

	# way_data = []

	# for way in ways:
	# 	id, tags = way
	# 	count+=1
	# 	print('Processing way {}, {} of {}...'.format(id, count, tot_ways))
	# 	LTSrank, assignment_code = assign_way_LTS(con, tags)
	# 	way_data.append((id, LTSrank, assignment_code))
	# 	#update_osm_ways(con, id, LTSrank, assignment_code)

	# update_osm_ways(con, way_data)

	# t1 = t.elapsed()
	# print('Processed {} ways in {}.'.format(tot_ways, t1-t0))

	count=0
	node_data = []
	for node in nodes:
		id, tags = node
		count+=1
		print('Processing node {}, {} of {}...'.format(id, count, tot_nodes))
		LTSrank, assignment_code = assign_node_LTS(con, id, tags)
		node_data.append((id, LTSrank, assignment_code))

	update_osm_nodes(con, node_data)
	t2 = t.elapsed()
	print('Processed {} nodes in {}'.format(tot_nodes, t2-t1))


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-s', '--stateids', required=False, default=None)
	args = parser.parse_args()

	if args.stateids:
		states = [args.stateids]
	else:
		states = ['27']

	con = psycopg2.connect(bikelts_dsn)
	con2 = psycopg2.connect(aodb_dsn)
	psycopg2.extras.register_hstore(con) #enable handling of hstore data type


	states = ["24",
	"56",
	"42",
	"39",
	"35",
	"44",
	"41",
	"55",
	"38",
	"32",
	"13",
	"36",
	"05",
	"20",
	"31",
	"49",
	"02",
	"28",
	"40",
	"54",
	"26",
	"08",
	"34",
	"10",
	"30",
	"53",
	"09",
	"06",
	"21",
	"25",
	"12",
	"16",
	"29",
	"15",
	"01",
	"45",
	"33",
	"46",
	"17",
	"47",
	"18",
	"19",
	"04",
	"27",
	"22",
	"11",
	"51",
	"48",
	"50",
	"23",
	"37"
]


	try:
		#for cbsaid in cbsaids:
			#process_cbsa(con, con2, cbsaid)
		for state in states:
			process_state(con, con2, state)
	except OSError, e:
		print(e)	
	finally:
		if con:
			con.close()
		if con2:
			con2.close()