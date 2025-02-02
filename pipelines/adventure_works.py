import dlt
import os
import typing as t

from dotenv import load_dotenv

from dlt.sources.rest_api.typing import RESTAPIConfig
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.helpers.rest_client.paginators import BaseNextUrlPaginator
from dlt.common import jsonpath
from requests import Response

class JSONLinkPaginator__odata(BaseNextUrlPaginator):
    """Locates the next page URL within the JSON response body. The key
    containing the URL can be specified using a JSON path.

    For example, suppose the JSON response from an API contains data items
    along with a 'pagination' object:

        {
            "items": [
                {"id": 1, "name": "item1"},
                {"id": 2, "name": "item2"},
                ...
            ],
            "pagination": {
                "next": "https://api.example.com/items?page=2"
            }
        }

    The link to the next page (`https://api.example.com/items?page=2`) is
    located in the 'next' key of the 'pagination' object. You can use
    `JSONLinkPaginator` to paginate through the API endpoint:

        from dlt.sources.helpers.rest_client import RESTClient
        client = RESTClient(
            base_url="https://api.example.com",
            paginator=JSONLinkPaginator(next_url_path="pagination.next")
        )

        @dlt.resource
        def get_data():
            for page in client.paginate("/posts"):
                yield page
    """

    def __init__(
        self,
        next_url_path: jsonpath.TJsonPath = "next",
    ):
        """
        Args:
            next_url_path (jsonpath.TJsonPath): The JSON path to the key
                containing the next page URL in the response body.
                Defaults to 'next'.
        """
        super().__init__()
        self.next_url_path = next_url_path

    def update_state(self, response: Response, data: t.Optional[t.List[t.Any]] = None) -> None:
        """Extracts the next page URL from the JSON response."""
        self._next_reference = response.json().get(self.next_url_path)

    def __str__(self) -> str:
        return super().__str__() + f": next_url_path: {self.next_url_path}"

@dlt.source(name="adventure_works")
def adventure_works_source() -> t.Any:
    source_config: RESTAPIConfig = {
        "client": {
            "base_url": "https://demodata.grapecity.com/adventureworks/odata/v1/",
        },
        "resource_defaults": {
            "write_disposition": "replace",
            "max_table_nesting": 0,
        },
        "resources": [
            {
                "name": "get__adventure_works__odata__v1__adresses",
                "table_name": "raw__adventure_works__adresses",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/Addresses",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__adress_types",
                "table_name": "raw__adventure_works__adress_types",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/AddressTypes",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__bill_of_materials",
                "table_name": "raw__adventure_works__bill_of_materials",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/BillOfMaterials",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__business_entity_adresses",
                "table_name": "raw__adventure_works__business_entity_adresses",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/BusinessEntityAddresses",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__business_entity_contacts",
                "table_name": "raw__adventure_works__business_entity_contacts",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/BusinessEntityContacts",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__contact_types",
                "table_name": "raw__adventure_works__contact_types",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/ContactTypes",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__country_regions",
                "table_name": "raw__adventure_works__country_regions",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/CountryRegions",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__credit_cards",
                "table_name": "raw__adventure_works__credit_cards",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/CreditCards",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__cultures",
                "table_name": "raw__adventure_works__cultures",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/Cultures",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__currencies",
                "table_name": "raw__adventure_works__currencies",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/Currencies",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__currency_rates",
                "table_name": "raw__adventure_works__currency_rates",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/CurrencyRates",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__customers",
                "table_name": "raw__adventure_works__customers",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/Customers",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__departments",
                "table_name": "raw__adventure_works__departments",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/Departments",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__email_addresses",
                "table_name": "raw__adventure_works__email_addresses",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/EmailAddresses",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__employee_department_histories",
                "table_name": "raw__adventure_works__employee_department_histories",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/EmployeeDepartmentHistories",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__employee_pay_histories",
                "table_name": "raw__adventure_works__employee_pay_histories",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/EmployeePayHistories",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__employees",
                "table_name": "raw__adventure_works__employees",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/Employees",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__illustrations",
                "table_name": "raw__adventure_works__illustrations",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/Illustrations",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__job_candidates",
                "table_name": "raw__adventure_works__job_candidates",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/JobCandidates",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__locations",
                "table_name": "raw__adventure_works__locations",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/Locations",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__person_phones",
                "table_name": "raw__adventure_works__person_phones",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/PersonPhones",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__persons",
                "table_name": "raw__adventure_works__persons",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/Persons",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__phone_number_types",
                "table_name": "raw__adventure_works__phone_number_types",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/PhoneNumberTypes",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_categories",
                "table_name": "raw__adventure_works__product_categories",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/ProductCategories",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_cost_histories",
                "table_name": "raw__adventure_works__product_cost_histories",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/ProductCostHistories",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_descriptions",
                "table_name": "raw__adventure_works__product_descriptions",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/ProductDescriptions",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_inventories",
                "table_name": "raw__adventure_works__product_inventories",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/ProductInventories",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_list_price_histories",
                "table_name": "raw__adventure_works__product_list_price_histories",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/ProductListPriceHistories",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_model_illustrations",
                "table_name": "raw__adventure_works__product_model_illustrations",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/ProductModelIllustrations",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_models",
                "table_name": "raw__adventure_works__product_models",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/ProductModels",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_photos",
                "table_name": "raw__adventure_works__product_photos",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/ProductPhotos",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_reviews",
                "table_name": "raw__adventure_works__product_reviews",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/ProductReviews",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__products",
                "table_name": "raw__adventure_works__products",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/Products",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_subcategories",
                "table_name": "raw__adventure_works__product_subcategories",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/ProductSubcategories",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_vendors",
                "table_name": "raw__adventure_works__product_vendors",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/ProductVendors",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__purchase_order_details",
                "table_name": "raw__adventure_works__purchase_order_details",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/PurchaseOrderDetails",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__purchase_order_headers",
                "table_name": "raw__adventure_works__purchase_order_headers",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/PurchaseOrderHeaders",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__sales_order_details",
                "table_name": "raw__adventure_works__sales_order_details",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/SalesOrderDetails",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__sales_order_headers",
                "table_name": "raw__adventure_works__sales_order_headers",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/SalesOrderHeaders",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__sales_person_quota_histories",
                "table_name": "raw__adventure_works__sales_person_quota_histories",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/SalesPersonQuotaHistories",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__sales_persons",
                "table_name": "raw__adventure_works__sales_persons",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/SalesPersons",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__sales_reasons",
                "table_name": "raw__adventure_works__sales_reasons",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/SalesReasons",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__sales_tax_rates",
                "table_name": "raw__adventure_works__sales_tax_rates",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/SalesTaxRates",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__sales_territories",
                "table_name": "raw__adventure_works__sales_territories",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/SalesTerritories",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__sales_territory_histories",
                "table_name": "raw__adventure_works__sales_territory_histories",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/SalesTerritoryHistories",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__scrap_reasons",
                "table_name": "raw__adventure_works__scrap_reasons",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/ScrapReasons",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__shifts",
                "table_name": "raw__adventure_works__shifts",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/Shifts",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__ship_methods",
                "table_name": "raw__adventure_works__ship_methods",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/ShipMethods",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__shopping_cart_items",
                "table_name": "raw__adventure_works__shopping_cart_items",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/ShoppingCartItems",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__specials_offers",
                "table_name": "raw__adventure_works__specials_offers",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/SpecialOffers",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__state_provinces",
                "table_name": "raw__adventure_works__state_provinces",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/StateProvinces",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__stores",
                "table_name": "raw__adventure_works__stores",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/Stores",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__transaction_histories",
                "table_name": "raw__adventure_works__transaction_histories",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/TransactionHistories",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__transaction_history_archives",
                "table_name": "raw__adventure_works__transaction_history_archives",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/TransactionHistoryArchives",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__unit_measures",
                "table_name": "raw__adventure_works__unit_measures",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/UnitMeasures",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__vendors",
                "table_name": "raw__adventure_works__vendors",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/Vendors",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__work_order_routings",
                "table_name": "raw__adventure_works__work_order_routings",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/WorkOrderRoutings",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__work_orders",
                "table_name": "raw__adventure_works__work_orders",
                "endpoint": {
                    "data_selector": "value",
                    "path": "/WorkOrders",
                    "paginator": JSONLinkPaginator__odata("@odata.nextLink"),
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