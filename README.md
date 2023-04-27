# Condos parser ## Installation

### Create .env file in `condos_parser` directory

```
touch .env
```

### env example:

```
# PAGINATION
SITE_PAGES='MQ,Mg,Mw,NA,NQ,Ng,Nw'

# TG BOT
TG_BOT_TOKEN='bot_token'
TG_CHAT_ID='chat_id'

# AIRTABLE
AIR_TABLE_TOKEN='token'
AIR_TABLE_API_KEY='api_key'
AIR_TABLE_BASE_ID='base_id'
MAIN_TABLE_ID='main_table_id'
AMENITIES_TABLE_ID='amenities_table_id'
UNITS_TABLE_ID='units_table_id'


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

