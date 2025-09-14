## Setup

### Run Producer
To run the producer (ingest raw battle logs of good players and sends to `battle-logs-topic` kafka topic):

```bash
sbt "run -m producer -t $BRAWL_STARS_API_TOKEN_THEDEAN"   
```

### Run Consumer
```bash
sbt "run -m producer -t $BRAWL_STARS_API_TOKEN_THEDEAN"   
```

