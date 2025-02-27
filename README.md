# Serverless Lakehouse

This project utilizes dlt, DuckDB, and SQLMesh, to create a serverless lakehouse by:
1. Extracting data from source via dlt.
2. Loading the data to delta files.
3. Reading the bronze using DuckDB.
4. Transforming the data using SQLMesh.
5. Extracting silver & gold from DuckDB with dlt.
6. Loading silver & gold to delta files.

It does this locally into `./lakehouse`, which could be replaced by a S3 bucket.

## Streamlit Dashboard
<img width="1440" alt="image" src="https://github.com/user-attachments/assets/61f70e7b-6f0d-46a3-ba14-2f516dae9575" />

## Architecture
```mermaid
architecture-beta
    service api(cloud)[Adventure Works API]
    service extract(server)[dlt]
    service consumption(cloud)[BI]

    group storage(cloud)[Iceberg]
        service bronze(disk)[Bronze] in storage
        service silver(disk)[Silver] in storage
        service gold(disk)[Gold] in storage

    group engine(server)[Spark]
        service transform(server)[SQLMesh] in engine

    api:R -- L:extract
    extract:R -- L:bronze

    bronze:T -- L:transform
    silver:T -- B:transform
    gold:T -- R:transform
    
    bronze:R -- L:silver
    silver:R -- L:gold

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
        raw__adventure_works__transaction_histories(["raw__adventure_works__transaction_histories"])
        raw__adventure_works__transaction_history_archives(["raw__adventure_works__transaction_history_archives"])
    end

    subgraph lakehouse.silver["lakehouse.silver"]
        direction LR
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
        bag__adventure_works__transactions(["bag__adventure_works__transactions"])
        int__adventure_works__inventories(["int__adventure_works__inventories"])
        uss_bridge(["uss_bridge"])
        uss_bridge__addresses(["uss_bridge__addresses"])
        uss_bridge__credit_cards(["uss_bridge__credit_cards"])
        uss_bridge__currency_rates(["uss_bridge__currency_rates"])
        uss_bridge__customers(["uss_bridge__customers"])
        uss_bridge__inventories(["uss_bridge__inventories"])
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
        _bridge__as_of_event(["_bridge__as_of_event"])
        _cube__metrics(["_cube__metrics"])
        addresses(["addresses"])
        calendar(["calendar"])
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
    raw__adventure_works__transaction_histories --> bag__adventure_works__transactions
    raw__adventure_works__transaction_history_archives --> bag__adventure_works__transactions

    %% lakehouse.silver -> lakehouse.silver
    bag__adventure_works__addresses --> uss_bridge__addresses
    bag__adventure_works__credit_cards --> uss_bridge__credit_cards
    bag__adventure_works__currency_rates --> uss_bridge__currency_rates
    bag__adventure_works__customers --> uss_bridge__customers
    bag__adventure_works__product_categories --> uss_bridge__product_categories
    bag__adventure_works__product_subcategories --> uss_bridge__product_subcategories
    bag__adventure_works__products --> uss_bridge__products
    bag__adventure_works__sales_order_details --> uss_bridge__sales_order_details
    bag__adventure_works__sales_order_headers --> uss_bridge__sales_order_details
    bag__adventure_works__sales_order_headers --> uss_bridge__sales_order_headers
    bag__adventure_works__sales_persons --> uss_bridge__sales_persons
    bag__adventure_works__sales_territories --> uss_bridge__sales_territories
    bag__adventure_works__ship_methods --> uss_bridge__ship_methods
    bag__adventure_works__special_offers --> uss_bridge__special_offers
    bag__adventure_works__state_provinces --> uss_bridge__state_provinces
    bag__adventure_works__transactions --> int__adventure_works__inventories
    int__adventure_works__inventories --> uss_bridge__inventories
    uss_bridge__addresses --> uss_bridge
    uss_bridge__addresses --> uss_bridge__sales_order_headers
    uss_bridge__credit_cards --> uss_bridge
    uss_bridge__credit_cards --> uss_bridge__sales_order_headers
    uss_bridge__currency_rates --> uss_bridge
    uss_bridge__currency_rates --> uss_bridge__sales_order_headers
    uss_bridge__customers --> uss_bridge
    uss_bridge__customers --> uss_bridge__sales_order_headers
    uss_bridge__inventories --> uss_bridge
    uss_bridge__product_categories --> uss_bridge
    uss_bridge__product_categories --> uss_bridge__product_subcategories
    uss_bridge__product_subcategories --> uss_bridge
    uss_bridge__product_subcategories --> uss_bridge__products
    uss_bridge__products --> uss_bridge
    uss_bridge__products --> uss_bridge__inventories
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
    uss_bridge --> _bridge__as_of_event
    uss_bridge --> calendar

    %% lakehouse.gold -> lakehouse.gold
    _bridge__as_of_event --> _cube__metrics
    addresses --> _cube__metrics
    calendar --> _cube__metrics
    credit_cards --> _cube__metrics
    currency_rates --> _cube__metrics
    customers --> _cube__metrics
    product_categories --> _cube__metrics
    product_subcategories --> _cube__metrics
    products --> _cube__metrics
    sales_order_details --> _cube__metrics
    sales_order_headers --> _cube__metrics
    sales_persons --> _cube__metrics
    sales_territories --> _cube__metrics
    ship_methods --> _cube__metrics
    special_offers --> _cube__metrics
    state_provinces --> _cube__metrics
    
    linkStyle default stroke:#666,stroke-width:2px

    %% Bronze shades
    classDef bronze_classic fill:#CD7F32,color:black
    classDef bronze_dark fill:#B87333,color:black
    classDef bronze_light fill:#E09756,color:black
    classDef bronze_antique fill:#966B47,color:black
    
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
