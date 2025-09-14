# mcp_server.py
import os
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from pymongo import MongoClient
from datetime import datetime
import uvicorn
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp")

app = FastAPI()

MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://milan:mk9913072585@cluster0.3qshu.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
DB_NAME = os.environ.get("DB_NAME", "marketing_db")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# Map logical names to collection handles
COLLECTIONS = {
    "retailer_daily_spend": db["retailer_daily_spend"],
    "retailer_keywords_daily": db["retailer_keywords_daily"],
    "retailer_page_type_daily": db["retailer_page_type_daily"],
    "retailer_product_daily": db["retailer_product_daily"],
    "budget": db["budget"],
    "brand": db["brand"],
    "retailer": db["retailer"],
    "campaign": db["campaign"],
}

# --- Models ---
class QueryPayload(BaseModel):
    query: Optional[str] = None
    kpi: Optional[str] = Field("ROAS", description="ROAS|profit|conversions")  # Fixed: was 'kip'
    from_date: Optional[str] = Field(None, alias="from")
    to_date: Optional[str] = Field(None, alias="to")
    channels: Optional[List[str]] = None
    constraints: Optional[Dict[str, Any]] = None
    top_n_campaigns: Optional[int] = 10
    collection: Optional[str] = None

    class Config:
        populate_by_name = True  # Fixed: was 'allow_population_by_field_name'

def _parse_iso(s: Optional[str]):
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(s, fmt)
        except:
            continue
    try:
        return datetime.fromisoformat(s)
    except:
        return None

def _get_collection(col_name: Optional[str]):
    if not col_name:
        return COLLECTIONS["retailer_daily_spend"]
    if col_name not in COLLECTIONS:
        raise HTTPException(status_code=400, detail=f"Unknown collection: {col_name}")
    return COLLECTIONS[col_name]

def _aggregate_generic(col, filter_q, group_by, metrics, top_n=10, sort_by=None):
    pipeline = []
    if filter_q:
        pipeline.append({"$match": filter_q})
    group_stage = {"_id": f"${group_by}"}
    for m in metrics:
        group_stage[m] = {"$sum": {"$ifNull": [f"${m}", 0]}}
    pipeline.append({"$group": group_stage})
    project_stage = {group_by: "$_id"}
    for m in metrics:
        project_stage[m] = 1
    # Add calculated fields
    if "ad_sales" in metrics and "ad_spend" in metrics:
        project_stage["roas"] = {"$cond": [{"$gt": ["$ad_spend", 0]}, {"$divide": ["$ad_sales", "$ad_spend"]}, None]}
    if "clicks" in metrics and "impressions" in metrics:
        project_stage["ctr"] = {"$cond": [{"$gt": ["$impressions", 0]}, {"$divide": ["$clicks", "$impressions"]}, None]}
    if "ad_spend" in metrics and "clicks" in metrics:
        project_stage["cpc"] = {"$cond": [{"$gt": ["$clicks", 0]}, {"$divide": ["$ad_spend", "$clicks"]}, None]}
    if "orders" in metrics and "clicks" in metrics:
        project_stage["conversion_rate"] = {"$cond": [{"$gt": ["$clicks", 0]}, {"$divide": ["$orders", "$clicks"]}, None]}
    pipeline.append({"$project": project_stage})
    if sort_by:
        pipeline.append({"$sort": {sort_by: -1}})
    if top_n:
        pipeline.append({"$limit": top_n})
    res = list(col.aggregate(pipeline))
    # Convert types
    for r in res:
        for k in metrics:
            r[k] = float(r.get(k, 0))
        for k in ("roas", "ctr", "cpc", "conversion_rate"):
            if k in r:
                v = r.get(k)
                r[k] = float(v) if v is not None else None
    return res

def _suggest_reallocation(channels_summary, kpi="ROAS", constraints=None):
    constraints = constraints or {}
    min_roas = constraints.get("min_roas")
    max_spend = constraints.get("max_spend")
    scores = {}
    for ch in channels_summary:
        roas = ch.get("roas") or 0.0
        conv = ch.get("conversion_rate") or 0.0
        score = roas * 0.7 + conv * 0.3
        if min_roas and roas < min_roas:
            score *= 0.5
        scores[ch.get("channel") or ch.get("retailer_id") or ch.get("retailer_name")] = max(score, 0.0) + 1e-6
    total_score = sum(scores.values()) or 1.0
    current_total = sum(ch.get("ad_spend", 0.0) for ch in channels_summary)
    target_total = current_total if not max_spend else min(current_total, max_spend)
    if max_spend and max_spend > current_total:
        target_total = max_spend
    suggested = []
    for ch in channels_summary:
        cur = ch.get("ad_spend", 0.0)
        ch_key = ch.get("channel") or ch.get("retailer_id") or ch.get("retailer_name")
        sc = scores.get(ch_key, 1e-6)
        s = (sc / total_score) * target_total
        lower = cur * 0.5
        upper = cur * 1.5 if cur > 0 else s
        s = max(lower, min(s, upper))
        suggested.append({
            "channel": ch_key,
            "current": round(cur, 2),
            "suggested": round(s, 2),
            "reason": f"Score-based (roas={ch.get('roas')}, conv_rate={ch.get('conversion_rate')})"
        })
    return suggested

@app.post("/mcp/query-data")
def query_data(payload: QueryPayload):
    try:
        # Handle both alias and field names
        from_date = payload.from_date if hasattr(payload, 'from_date') and payload.from_date else None
        to_date = payload.to_date if hasattr(payload, 'to_date') and payload.to_date else None
        
        dt_from = _parse_iso(from_date)
        dt_to = _parse_iso(to_date)
        col = _get_collection(payload.collection)
        q = {}
        
        # Date filter - handle MongoDB ISODate objects
        if dt_from or dt_to:
            q["date"] = {}
            if dt_from:
                # Use actual datetime object for MongoDB date comparison
                q["date"]["$gte"] = dt_from
            if dt_to:
                # Add 23:59:59 to include the entire end date
                end_datetime = dt_to.replace(hour=23, minute=59, second=59, microsecond=999999)
                q["date"]["$lte"] = end_datetime
        
        # Channel/source filter - more flexible matching
        if payload.channels:
            # Convert channel names to potential retailer_ids
            channel_conditions = []
            for ch in payload.channels:
                channel_conditions.extend([
                    {"channel": {"$regex": ch, "$options": "i"}},
                    {"source": {"$regex": ch, "$options": "i"}},
                    {"retailer_name": {"$regex": ch, "$options": "i"}},
                    {"retailer_id": ch},  # Exact string match
                ])
                # Try numeric conversion
                try:
                    if ch.isdigit():
                        channel_conditions.append({"retailer_id": int(ch)})
                except:
                    pass
                
                # Map common channel names to retailer_ids based on your data
                channel_mapping = {
                    "amazon": [2, 33, "amazon", "Amazon"],
                    "walmart": [16, 21, "walmart", "Walmart"],
                    "kroger": [32, "kroger", "Kroger"],
                    "meijer": [21, "meijer", "Meijer"]
                }
                
                ch_lower = ch.lower()
                if ch_lower in channel_mapping:
                    for mapped_id in channel_mapping[ch_lower]:
                        if isinstance(mapped_id, int):
                            channel_conditions.append({"retailer_id": mapped_id})
                        else:
                            channel_conditions.append({"retailer_name": {"$regex": mapped_id, "$options": "i"}})
            
            q["$or"] = channel_conditions
        
        # Choose group_by and metrics based on collection
        if payload.collection == "retailer_keywords_daily":
            group_by = "keywords"
            metrics = ["impressions", "clicks", "ad_spend", "ad_sales", "ad_units"]
        elif payload.collection == "retailer_page_type_daily":
            group_by = "page_type"
            metrics = ["impressions", "clicks", "ad_spend", "ad_sales", "ad_units"]
        elif payload.collection == "retailer_product_daily":
            group_by = "product_id"
            metrics = ["impressions", "clicks", "ad_spend", "ad_sales", "ad_units"]
        else:  # Default: retailer_daily_spend or similar
            group_by = "retailer_id"
            metrics = ["impressions", "clicks", "ad_spend", "ad_sales", "ad_units"]
        
        summary = _aggregate_generic(col, q, group_by, metrics, top_n=payload.top_n_campaigns, sort_by="ad_sales")
        
        # Suggest reallocation (only for spend-based collections)
        suggested = _suggest_reallocation(summary, kpi=payload.kpi, constraints=payload.constraints or {})
        
        metadata = {
            "query": payload.query, 
            "kpi": payload.kpi, 
            "time_range": {"from": from_date, "to": to_date}, 
            "collection": payload.collection,
            "total_records_found": len(summary),
            "filters_applied": q
        }
        explainability = "Heuristic proportional allocation using ROAS and conversion rate (weights: ROAS 0.7, conv 0.3). Bounds: +/-50% per channel."
        
        return {
            "metadata": metadata,
            "summary": summary,
            "suggested_reallocation": suggested,
            "explainability": explainability,
            "confidence": 0.75
        }
    except Exception as e:
        logger.exception("query_data error")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health():
    return {"status": "ok", "db": DB_NAME, "collections": list(COLLECTIONS.keys())}

# Debug endpoint to check data
@app.get("/debug/sample-data")
def debug_sample_data(collection: str = "retailer_daily_spend"):
    try:
        col = _get_collection(collection)
        sample = list(col.find().limit(3))
        return {"collection": collection, "sample_records": sample}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)