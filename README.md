# Serverless Lakehouse

This project utilizes dlt, DuckDB, and SQLMesh, to create a serverless lakehouse by:
1. Extracting data from source via dlt.
2. Loading the data to delta files.
3. Reading the bronze using DuckDB.
4. Transforming the data using SQLMesh.
5. Extracting silver & gold from DuckDB with dlt.
6. Loading silver & gold to delta files.

It does this locally into `./lakehouse`, which could be replaced by a S3 bucket.

## Architecture
```mermaid
architecture-beta
    service api(cloud)[Adventure Works API]
    service extract(server)[dlt]
    service load(server)[SQLMesh]
    service transform(server)[SQLMesh]
    service export_silver(server)[dlt]
    service export_gold(server)[dlt]
    service consumption(cloud)[BI]

    group storage(cloud)[Storage]
        service bronze(disk)[Bronze] in storage
        service silver(disk)[Silver] in storage
        service gold(disk)[Gold] in storage

    group engine(database)[DuckDB]
        service silver_view(database)[Silver] in engine
        service gold_view(database)[Gold] in engine

    api:R -- L:extract
    extract:R -- L:bronze
    bronze:T -- B:load
    load:T -- L:silver_view
    silver_view:T -- L:transform
    transform:B -- T:gold_view
    silver_view:B -- T:export_silver
    export_silver:B -- T:silver
    gold_view:B -- T:export_gold
    export_gold:B -- T:gold
    gold:R -- L:consumption
```

## Lineage / DAG
```mermaid
flowchart LR
    subgraph lakehouse.bronze["lakehouse.bronze"]
        direction LR
        raw__adventure_works__addresses(["raw__adventure_works__addresses"])
        raw__adventure_works__credit_cards(["raw__adventure_works__credit_cards"])
        raw__adventure_works__currency_rates(["raw__adventure_works__currency_rates"])
        raw__adventure_works__customers(["raw__adventure_works__customers"])
        raw__adventure_works__product_categories(["raw__adventure_works__product_categories"])
        raw__adventure_works__product_subcategories(["raw__adventure_works__product_subcategories"])
        raw__adventure_works__products(["raw__adventure_works__products"])
        raw__adventure_works__sales_order_details(["raw__adventure_works__sales_order_details"])
        raw__adventure_works__sales_order_headers(["raw__adventure_works__sales_order_headers"])
        raw__adventure_works__sales_persons(["raw__adventure_works__sales_persons"])
        raw__adventure_works__sales_territories(["raw__adventure_works__sales_territories"])
        raw__adventure_works__ship_methods(["raw__adventure_works__ship_methods"])
        raw__adventure_works__special_offers(["raw__adventure_works__special_offers"])
        raw__adventure_works__state_provinces(["raw__adventure_works__state_provinces"])
    end

    subgraph lakehouse.silver["lakehouse.silver"]
        direction LR

        subgraph bags
            bag__adventure_works__addresses(["bag__adventure_works__addresses"])
            bag__adventure_works__credit_cards(["bag__adventure_works__credit_cards"])
            bag__adventure_works__currency_rates(["bag__adventure_works__currency_rates"])
            bag__adventure_works__customers(["bag__adventure_works__customers"])
            bag__adventure_works__product_categories(["bag__adventure_works__product_categories"])
            bag__adventure_works__product_subcategories(["bag__adventure_works__product_subcategories"])
            bag__adventure_works__products(["bag__adventure_works__products"])
            bag__adventure_works__sales_order_details(["bag__adventure_works__sales_order_details"])
            bag__adventure_works__sales_order_headers(["bag__adventure_works__sales_order_headers"])
            bag__adventure_works__sales_persons(["bag__adventure_works__sales_persons"])
            bag__adventure_works__sales_territories(["bag__adventure_works__sales_territories"])
            bag__adventure_works__ship_methods(["bag__adventure_works__ship_methods"])
            bag__adventure_works__special_offers(["bag__adventure_works__special_offers"])
            bag__adventure_works__state_provinces(["bag__adventure_works__state_provinces"])
        end
        
        uss_bridge(["uss_bridge"])
        uss_bridge__addresses(["uss_bridge__addresses"])
        uss_bridge__credit_cards(["uss_bridge__credit_cards"])
        uss_bridge__currency_rates(["uss_bridge__currency_rates"])
        uss_bridge__customers(["uss_bridge__customers"])
        uss_bridge__product_categories(["uss_bridge__product_categories"])
        uss_bridge__product_subcategories(["uss_bridge__product_subcategories"])
        uss_bridge__products(["uss_bridge__products"])
        uss_bridge__sales_order_details(["uss_bridge__sales_order_details"])
        uss_bridge__sales_order_headers(["uss_bridge__sales_order_headers"])
        uss_bridge__sales_persons(["uss_bridge__sales_persons"])
        uss_bridge__sales_territories(["uss_bridge__sales_territories"])
        uss_bridge__ship_methods(["uss_bridge__ship_methods"])
        uss_bridge__special_offers(["uss_bridge__special_offers"])
        uss_bridge__state_provinces(["uss_bridge__state_provinces"])
    end

    subgraph lakehouse.gold["lakehouse.gold"]
        direction LR
        _bridge__as_is(["_bridge__as_is"])
        _bridge__as_of(["_bridge__as_of"])
        addresses(["addresses"])
        credit_cards(["credit_cards"])
        currency_rates(["currency_rates"])
        customers(["customers"])
        product_categories(["product_categories"])
        product_subcategories(["product_subcategories"])
        products(["products"])
        sales_order_details(["sales_order_details"])
        sales_order_headers(["sales_order_headers"])
        sales_persons(["sales_persons"])
        sales_territories(["sales_territories"])
        ship_methods(["ship_methods"])
        special_offers(["special_offers"])
        state_provinces(["state_provinces"])
    end

    %% lakehouse.bronze -> lakehouse.silver
    raw__adventure_works__addresses --> bag__adventure_works__addresses
    raw__adventure_works__credit_cards --> bag__adventure_works__credit_cards
    raw__adventure_works__currency_rates --> bag__adventure_works__currency_rates
    raw__adventure_works__customers --> bag__adventure_works__customers
    raw__adventure_works__product_categories --> bag__adventure_works__product_categories
    raw__adventure_works__product_subcategories --> bag__adventure_works__product_subcategories
    raw__adventure_works__products --> bag__adventure_works__products
    raw__adventure_works__sales_order_details --> bag__adventure_works__sales_order_details
    raw__adventure_works__sales_order_headers --> bag__adventure_works__sales_order_headers
    raw__adventure_works__sales_persons --> bag__adventure_works__sales_persons
    raw__adventure_works__sales_territories --> bag__adventure_works__sales_territories
    raw__adventure_works__ship_methods --> bag__adventure_works__ship_methods
    raw__adventure_works__special_offers --> bag__adventure_works__special_offers
    raw__adventure_works__state_provinces --> bag__adventure_works__state_provinces

    %% lakehouse.silver -> lakehouse.silver
    bag__adventure_works__addresses --> uss_bridge__addresses
    bag__adventure_works__credit_cards --> uss_bridge__credit_cards
    bag__adventure_works__currency_rates --> uss_bridge__currency_rates
    bag__adventure_works__customers --> uss_bridge__customers
    bag__adventure_works__product_categories --> uss_bridge__product_categories
    bag__adventure_works__product_subcategories --> uss_bridge__product_subcategories
    bag__adventure_works__products --> uss_bridge__products
    bag__adventure_works__sales_order_details --> uss_bridge__sales_order_details
    bag__adventure_works__sales_order_headers --> uss_bridge__sales_order_headers
    bag__adventure_works__sales_persons --> uss_bridge__sales_persons
    bag__adventure_works__sales_territories --> uss_bridge__sales_territories
    bag__adventure_works__ship_methods --> uss_bridge__ship_methods
    bag__adventure_works__special_offers --> uss_bridge__special_offers
    bag__adventure_works__state_provinces --> uss_bridge__state_provinces
    uss_bridge__addresses --> uss_bridge
    uss_bridge__addresses --> uss_bridge__sales_order_headers
    uss_bridge__credit_cards --> uss_bridge
    uss_bridge__credit_cards --> uss_bridge__sales_order_headers
    uss_bridge__currency_rates --> uss_bridge
    uss_bridge__currency_rates --> uss_bridge__sales_order_headers
    uss_bridge__customers --> uss_bridge
    uss_bridge__customers --> uss_bridge__sales_order_headers
    uss_bridge__product_categories --> uss_bridge
    uss_bridge__product_categories --> uss_bridge__product_subcategories
    uss_bridge__product_subcategories --> uss_bridge
    uss_bridge__product_subcategories --> uss_bridge__products
    uss_bridge__products --> uss_bridge
    uss_bridge__products --> uss_bridge__sales_order_details
    uss_bridge__sales_order_details --> uss_bridge
    uss_bridge__sales_order_headers --> uss_bridge
    uss_bridge__sales_order_headers --> uss_bridge__sales_order_details
    uss_bridge__sales_persons --> uss_bridge
    uss_bridge__sales_persons --> uss_bridge__sales_order_headers
    uss_bridge__sales_territories --> uss_bridge
    uss_bridge__sales_territories --> uss_bridge__state_provinces
    uss_bridge__ship_methods --> uss_bridge
    uss_bridge__ship_methods --> uss_bridge__sales_order_headers
    uss_bridge__special_offers --> uss_bridge
    uss_bridge__special_offers --> uss_bridge__sales_order_details
    uss_bridge__state_provinces --> uss_bridge
    uss_bridge__state_provinces --> uss_bridge__addresses

    %% lakehouse.silver -> lakehouse.gold
    bag__adventure_works__addresses --> addresses
    bag__adventure_works__credit_cards --> credit_cards
    bag__adventure_works__currency_rates --> currency_rates
    bag__adventure_works__customers --> customers
    bag__adventure_works__product_categories --> product_categories
    bag__adventure_works__product_subcategories --> product_subcategories
    bag__adventure_works__products --> products
    bag__adventure_works__sales_order_details --> sales_order_details
    bag__adventure_works__sales_order_headers --> sales_order_headers
    bag__adventure_works__sales_persons --> sales_persons
    bag__adventure_works__sales_territories --> sales_territories
    bag__adventure_works__ship_methods --> ship_methods
    bag__adventure_works__special_offers --> special_offers
    bag__adventure_works__state_provinces --> state_provinces
    uss_bridge --> _bridge__as_is
    uss_bridge --> _bridge__as_of

    linkStyle default stroke:#666,stroke-width:2px

    %% Bronze shades
    classDef bronze_classic fill:#CD7F32,color:white
    classDef bronze_dark fill:#B87333,color:white
    classDef bronze_light fill:#E09756,color:white
    classDef bronze_antique fill:#966B47,color:white
    
    %% Silver shades
    classDef silver_classic fill:#C0C0C0,color:black
    classDef silver_dark fill:#A8A8A8,color:black
    classDef silver_light fill:#D8D8D8,color:black
    classDef silver_antique fill:#B4B4B4,color:black
    
    %% Gold shades
    classDef gold_classic fill:#FFD700,color:black
    classDef gold_dark fill:#DAA520,color:black
    classDef gold_light fill:#FFE55C,color:black
    classDef gold_antique fill:#CFB53B,color:black

    class lakehouse.bronze bronze_classic

    class lakehouse.silver silver_classic
    class bags silver_antique

    class lakehouse.gold gold_classic
```

## ERDs - Oriented Data Models
### Bronze
```mermaid
flowchart LR
    raw__adventure_works__sales_order_details --> raw__adventure_works__products
    raw__adventure_works__sales_order_details --> raw__adventure_works__sales_order_headers
    raw__adventure_works__sales_order_details --> raw__adventure_works__special_offers
    
    raw__adventure_works__products --> raw__adventure_works__product_subcategories
    raw__adventure_works__product_subcategories --> raw__adventure_works__product_categories

    raw__adventure_works__sales_order_headers --> raw__adventure_works__addresses
    raw__adventure_works__sales_order_headers --> raw__adventure_works__credit_cards
    raw__adventure_works__sales_order_headers --> raw__adventure_works__currency_rates
    raw__adventure_works__sales_order_headers --> raw__adventure_works__customers
    raw__adventure_works__sales_order_headers --> raw__adventure_works__persons
    raw__adventure_works__sales_order_headers --> raw__adventure_works__ship_methods
    raw__adventure_works__customers --> raw__adventure_works__sales_territories
    raw__adventure_works__sales_order_headers --> raw__adventure_works__sales_territories
    
    raw__adventure_works__customers --> raw__adventure_works__persons
    raw__adventure_works__customers --> raw__adventure_works__stores
    
    raw__adventure_works__sales_territories --> raw__adventure_works__state_provinces
    
    raw__adventure_works__stores --> raw__adventure_works__persons
```

### Silver
Under construction

### Gold
```mermaid
flowchart LR
    _bridge --> customers
    _bridge --> products
    _bridge --> sales_order_details
    _bridge --> sales_order_headers
    _bridge --> sales_persons
    _bridge --> sales_territories
    _bridge --> stores
```