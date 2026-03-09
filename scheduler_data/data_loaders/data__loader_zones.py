@data_loader
def load_data(*args, **kwargs):
    import urllib.request
    import csv
    import io
    from datetime import datetime

    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    with urllib.request.urlopen(url) as response:
        content = response.read().decode("utf-8")

    reader = csv.DictReader(io.StringIO(content))

    rows = []
    for r in reader:
        rows.append({
            "location_id": int(r["LocationID"]),
            "borough": r["Borough"],
            "zone": r["Zone"],
            "service_zone": r["service_zone"],
            "ingest_ts": datetime.utcnow().isoformat()
        })
    
    print("TOTAL ROWS:", len(rows))
    print("SAMPLE:", rows[:3])
    return rows


    



