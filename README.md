## Setup

### Run Producer
To run the producer (ingest raw battle logs of good players and sends to `battle-logs-topic` kafka topic):

```bash
sbt "run -m producer -t $BRAWL_STARS_API_TOKEN_THEDEAN"   
```

### Run Consumers

Bronze Consumer writes raw battle logs as JSONL
```bash
sbt "run -m consumerBronze"   
```

Silver Consumer processes raw battle logs and writes to Parquet
```bash
sbt "run -m consumerSilver"   
```

Good Player Augmenter processes 
```bash
sbt "run -m GpaInit"
   ```
```bash
sbt "run -m GpaUpdate"
   ```

