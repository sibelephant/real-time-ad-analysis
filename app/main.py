import json
import os
import time
import random
import uuid
import asyncio
from typing import Optional
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from kafka import KafkaProducer
from pinotdb import connect

app = FastAPI(title="Ad Analysis API")

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ad-events")
PINOT_BROKER = os.getenv("PINOT_BROKER", "localhost:8099")

producer = None

class GenerationStatus(BaseModel):
    is_running: bool

is_generating = False

def get_producer():
    global producer
    if producer is None:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    return producer

async def generate_events():
    global is_generating
    campaigns = ["camp_spring", "camp_summer", "camp_fall", "camp_winter"]
    ads = [f"ad_{i}" for i in range(1, 11)]
    
    producer = get_producer()
    
    while is_generating:
        event = {
            "event_id": str(uuid.uuid4()),
            "campaign_id": random.choice(campaigns),
            "ad_id": random.choice(ads),
            "user_id": f"user_{random.randint(1, 1000)}",
            "event_type": random.choices(["impression", "click"], weights=[90, 10])[0],
            "event_time": int(time.time() * 1000)
        }
        producer.send(KAFKA_TOPIC, event)
        await asyncio.sleep(0.1)  # 10 events per second

@app.post("/start_generation")
async def start_generation(background_tasks: BackgroundTasks):
    global is_generating
    if not is_generating:
        is_generating = True
        background_tasks.add_task(generate_events)
        return {"status": "started"}
    return {"status": "already running"}

@app.post("/stop_generation")
async def stop_generation():
    global is_generating
    is_generating = False
    return {"status": "stopped"}

@app.get("/status", response_model=GenerationStatus)
async def status():
    return GenerationStatus(is_running=is_generating)

@app.get("/analytics/summary")
async def get_summary():
    try:
        conn = connect(host=PINOT_BROKER.split(':')[0], port=int(PINOT_BROKER.split(':')[1]), path="/query/sql", scheme="http")
        curs = conn.cursor()
        
        query = """
            SELECT 
                campaign_id,
                count(*) as total_events,
                SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END) as impressions,
                SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as clicks
            FROM ad_events
            GROUP BY campaign_id
            ORDER BY impressions DESC
        """
        curs.execute(query)
        
        results = []
        for row in curs:
            results.append({
                "campaign_id": row[0],
                "total_events": row[1],
                "impressions": row[2],
                "clicks": row[3],
                "ctr": (row[3] / row[2] * 100) if row[2] > 0 else 0
            })
            
        return {"data": results}
    except Exception as e:
        return {"error": str(e)}

@app.get("/analytics/realtime")
async def get_realtime_events(limit: int = 10):
    try:
        conn = connect(host=PINOT_BROKER.split(':')[0], port=int(PINOT_BROKER.split(':')[1]), path="/query/sql", scheme="http")
        curs = conn.cursor()
        
        query = f"""
            SELECT event_id, campaign_id, event_type, event_time
            FROM ad_events
            ORDER BY event_time DESC
            LIMIT {limit}
        """
        curs.execute(query)
        
        results = []
        for row in curs:
            results.append({
                "event_id": row[0],
                "campaign_id": row[1],
                "event_type": row[2],
                "event_time": row[3]
            })
            
        return {"data": results}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
