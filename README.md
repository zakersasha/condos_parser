# Condos parser ## Installation

### Create .env file in `condos_parser` directory

```
touch .env
```

### env example:
```
SITE_PAGES='MQ,Mg,Mw,NA,NQ,Ng,Nw'

TG_BOT_TOKEN='token'

TG_CHAT_ID='chat_id'
```

### Run locally:

```
in condos_parser directory:

- pip install -r requirements.txt
- python run.py
```

### Run via docker-compose:

```
in condos_parser directory:

docker-compose -f docker-compose.yml up --build 
                    -- or --
docker-compose -f docker-compose.yml up --build -d 
```

