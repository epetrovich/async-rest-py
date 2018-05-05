from aiohttp import web
import asyncio
import asyncpg
import csv
import datetime
import sys
import base64
import math
import statistics
from collections import namedtuple
from operator import attrgetter
from scipy.spatial import distance
import io
from PIL import Image
import matplotlib.pyplot as plt


rides_per_user = 5

async def db_init():
    conn = await asyncpg.connect(user='test', password='test',
                                 database='test', host='localhost')

    await conn.execute('''
        DROP TABLE IF EXISTS rides
    ''')
    await conn.execute('''
        CREATE TABLE rides(
            id serial PRIMARY KEY,
            user_id text,
            start_x decimal(8,3),
            start_y decimal(8,3),
            stop_x  decimal(8,3),
            stop_y  decimal(8,3),
            start_time timestamp,
            stop_time timestamp
        )
    ''')
    await conn.close()

async def db_store(user_id, start_x, start_y, stop_x, stop_y, start_time, stop_time):

    conn = await asyncpg.connect(user='test', password='test',
                                 database='test', host='localhost')

    rides_count = await conn.fetchval('SELECT count(*) FROM rides where user_id=$1', user_id)
    if rides_count >= rides_per_user:
            print("Dropping " + str(rides_count - rides_per_user + 1) + " records for user_id " + user_id )
            await conn.execute('''DELETE FROM rides WHERE id in ( select id from rides where user_id=$1 ORDER BY start_time ASC LIMIT $2);''', user_id, rides_count - rides_per_user + 1 )

    await conn.execute('''
        INSERT INTO rides(user_id, start_x, start_y, stop_x, stop_y, start_time, stop_time) VALUES($1, $2, $3, $4, $5, $6, $7)
    ''', user_id, start_x, start_y, stop_x, stop_y, datetime.datetime.fromtimestamp(int(start_time)), datetime.datetime.fromtimestamp(int(stop_time)))

    await conn.close()



async def db_stats():
    conn = await asyncpg.connect(user='test', password='test',
                                 database='test', host='localhost')
    data = await conn.fetch('SELECT * FROM rides')
    await conn.close()
    return data

async def db_chart(user_id):
    conn = await asyncpg.connect(user='test', password='test',
                                 database='test', host='localhost')
    data = await conn.fetch('SELECT * FROM rides WHERE user_id=$1', user_id)
    await conn.close()
    return data

def euq_distance_dispertion(rides):

    dst = []
    for ride in rides:
        dst.append(float(distance.euclidean((ride[0], ride[1]), (ride[2] ,ride[3]))))
    return statistics.pvariance(dst)



async def init_handler(request):
    text = "database init complete!"
    await db_init()
    return web.Response(text=text)

async def store_handler(request):
    user_id = request.rel_url.query['user_id']
    start_x = request.rel_url.query['start_x']
    stop_x = request.rel_url.query['stop_x']
    start_y = request.rel_url.query['start_y']
    stop_y = request.rel_url.query['stop_y']
    start_time = request.rel_url.query['start_time']
    stop_time = request.rel_url.query['stop_time']

    text = user_id + " " + start_x + " " + start_y + " " + stop_x + " " + stop_y + " " + start_time + " " + stop_time + "\n"
    await db_store(user_id, start_x, start_y, stop_x, stop_y, start_time, stop_time)
    return web.Response(text=text)

async def stats_handler(request):
    data = await db_stats()
    ride = namedtuple('ride', ['user_id', 'start_x', 'stop_x', 'start_y', 'stop_y', 'start_time', 'stop_time', 'distance'])
    rides = []
    for row in data:
        rides.append(ride(user_id=row['user_id'], start_x=row['start_x'], start_y=row['start_y'], stop_x=row['stop_x'], stop_y=row['stop_y'],
                     start_time=row['start_time'], stop_time=row['stop_time'], distance=math.hypot(row['stop_x']-row['start_x'], row['stop_y']-row['start_y'])))
    rides = sorted(rides, key=attrgetter('distance'))

    f = io.StringIO()
    w = csv.writer(f)
    for sorted_ride in rides:
        w.writerow([sorted_ride[1], sorted_ride[3], sorted_ride[2],sorted_ride[4],sorted_ride[0],sorted_ride[7]])

    return web.Response(text=str(f.getvalue()))


async def chart_handler(request):

    user_id = request.rel_url.query['user_id']
    ride = namedtuple('ride', ['start_x', 'stop_x', 'start_y', 'stop_y'])
    rides = []
    data = await db_chart(user_id)
    x=0
    plot_x = []
    plot_y = []
    for row in data:
        x+=1
        rides.append(ride(start_x=row['start_x'],start_y=row['start_y'],stop_x=row['stop_x'], stop_y=row['stop_y']))
        plot_x.append(x)
        plot_y.append(euq_distance_dispertion(rides))

    plt.figure()
    plt.scatter(plot_x, plot_y)
    plt.title("user_id:" + str(user_id) + " total_rides:" + str(rides_per_user))
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    resp = web.StreamResponse(status=200,
                              reason='OK',
                              headers={'Content-Type': 'img/png'})
    await resp.prepare(request)
    resp.write(buf.getvalue())
    return resp


app = web.Application()
app.router.add_get('/init',  init_handler)
app.router.add_get('/store', store_handler)
app.router.add_get('/stats', stats_handler)
app.router.add_get('/chart', chart_handler)

web.run_app(app)
