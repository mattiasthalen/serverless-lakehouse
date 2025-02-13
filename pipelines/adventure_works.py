import dlt
import os
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
                "paginator": ODataLinkPaginator("@odata.nextLink"),
                "data_selector": "value",
                "params": {
                    "$count": "true",
                    "$orderby": "ModifiedDate",
                },
                "incremental": {
                    "start_param": "$filter",
                    "cursor_path": "ModifiedDate",
                    "initial_value": "2014-01-01T00:00:00Z",
                    "convert": lambda date: f"ModifiedDate gt {date}"
                },
            },
        },
        "resources": [
            {
                "name": "get__adventure_works__odata__v1__sales_order_details",
                "table_name": "raw__adventure_works__sales_order_details",
                "primary_key": "SalesOrderDetailId",
                "endpoint": {
                    "path": "/SalesOrderDetails",
                },
            },
            {
                "name": "get__adventure_works__odata__v1__sales_order_headers",
                "table_name": "raw__adventure_works__sales_order_headers",
                "primary_key": "SalesOrderId",
                "endpoint": {
                    "path": "/SalesOrderHeaders",
                },
            },
        ],
    }

    yield from rest_api_resources(source_config)

def load_adventure_works() -> None:
    load_dotenv()
    
    duckdb_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "adventure_works.duckdb")
    
    pipeline = dlt.pipeline(
        pipeline_name="adventure_works",
        destination=dlt.destinations.duckdb(duckdb_path),
        dataset_name="bronze",
        progress="enlighten",
        export_schema_path="./pipelines/schemas/export",
        import_schema_path="./pipelines/schemas/import",
        dev_mode=False
    )

    source = adventure_works_source()
    
    load_info = pipeline.run(source)
    print(load_info)

if __name__ == "__main__":
    load_adventure_works()