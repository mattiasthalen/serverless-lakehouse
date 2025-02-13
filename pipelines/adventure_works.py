import dlt
import typing as t

from dotenv import load_dotenv

from dlt.sources.rest_api.typing import RESTAPIConfig
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator
from requests import Response

class ODataLinkPaginator(JSONLinkPaginator):
    def __init__(
        self,
        next_url_path = "@odata.nextLink",
    ):
        super().__init__()
        self.next_url_path = next_url_path

    def update_state(self, response: Response, data: t.Optional[t.List[t.Any]] = None) -> None:
        """Extracts the next page URL from the JSON response."""
        self._next_reference = response.json().get(self.next_url_path)
        print(f"Next page URL: {self._next_reference}")

@dlt.source(name="adventure_works")
def adventure_works_source() -> t.Any:
    source_config: RESTAPIConfig = {
        "client": {
            "base_url": "https://demodata.grapecity.com/adventureworks/odata/v1/",
        },
        "resource_defaults": {
            "write_disposition": "append",
            "max_table_nesting": 0,
            "endpoint": {
                "data_selector": "value",
                "params": {
                    "$count": "false",
                    "$orderby": "ModifiedDate",
                },
                "incremental": {
                    "start_param": "$filter",
                    "cursor_path": "ModifiedDate",
                    "initial_value": "1970-01-01",
                    "convert": lambda date: f"ModifiedDate ge {date}",
                    "range_start": "closed",
                    "on_cursor_value_missing": "include",
                    "row_order": "asc",
                },
            },
        },
        "resources": [
            {
                "name": "get__adventure_works__odata__v1__addresses",
                "table_name": "raw__adventure_works__addresses",
                "primary_key": "AddressId",
                "endpoint": {
                    "path": "/Addresses",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__credit_cards",
                "table_name": "raw__adventure_works__credit_cards",
                "primary_key": "CreditCardId",
                "endpoint": {
                    "path": "/CreditCards",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__currency_rates",
                "table_name": "raw__adventure_works__currency_rates",
                "primary_key": "CurrencyRateId",
                "endpoint": {
                    "path": "/CurrencyRates",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__customers",
                "table_name": "raw__adventure_works__customers",
                "primary_key": "CustomerId",
                "endpoint": {
                    "path": "/Customers",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__persons",
                "table_name": "raw__adventure_works__persons",
                "primary_key": "BusinessEntityId",
                "endpoint": {
                    "path": "/Persons",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__products",
                "table_name": "raw__adventure_works__products",
                "primary_key": "ProductId",
                "endpoint": {
                    "path": "/Products",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_categories",
                "table_name": "raw__adventure_works__product_categories",
                "primary_key": "ProductCategoryId",
                "endpoint": {
                    "path": "/ProductCategories",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__sales_order_details",
                "table_name": "raw__adventure_works__sales_order_details",
                "primary_key": "SalesOrderDetailId",
                "endpoint": {
                    "path": "/SalesOrderDetails",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__sales_order_headers",
                "table_name": "raw__adventure_works__sales_order_headers",
                "primary_key": "SalesOrderId",
                "endpoint": {
                    "path": "/SalesOrderHeaders",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__sales_persons",
                "table_name": "raw__adventure_works__sales_persons",
                "primary_key": "BusinessEntityId",
                "endpoint": {
                    "path": "/SalesPersons",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__sales_territories",
                "table_name": "raw__adventure_works__sales_territories",
                "primary_key": "TerritoryId",
                "endpoint": {
                    "path": "/SalesTerritories",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__ship_methods",
                "table_name": "raw__adventure_works__ship_methods",
                "primary_key": "ShipMethodId",
                "endpoint": {
                    "path": "/ShipMethods",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__special_offers",
                "table_name": "raw__adventure_works__special_offers",
                "primary_key": "SpecialOfferId",
                "endpoint": {
                    "path": "/SpecialOffers",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__state_provinces",
                "table_name": "raw__adventure_works__state_provinces",
                "primary_key": "StateProvinceId",
                "endpoint": {
                    "path": "/StateProvinces",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__stores",
                "table_name": "raw__adventure_works__stores",
                "primary_key": "BusinessEntityId",
                "endpoint": {
                    "path": "/Stores",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
        ],
    }

    yield from rest_api_resources(source_config)

def load_adventure_works() -> None:
    load_dotenv()
    
    pipeline = dlt.pipeline(
        pipeline_name="adventure_works",
        destination=dlt.destinations.filesystem("./lakehouse"),
        dataset_name="bronze",
        progress="enlighten",
        export_schema_path="./pipelines/schemas/export",
        import_schema_path="./pipelines/schemas/import",
        dev_mode=False
    )

    source = adventure_works_source()
    
    load_info = pipeline.run(source, table_format="delta")
    print(load_info)

if __name__ == "__main__":
    load_adventure_works()